package com.evolutiongaming.kafka.journal.ally.cassandra

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import com.datastax.driver.core._
import com.datastax.driver.core.policies.{LoggingRetryPolicy, RetryPolicy}
import com.evolutiongaming.cassandra.Helpers._
import com.evolutiongaming.cassandra.NextHostRetryPolicy
import com.evolutiongaming.kafka.journal.Alias._
import com.evolutiongaming.kafka.journal.SeqRange
import com.evolutiongaming.kafka.journal.ally.{AllyDb, AllyRecord, AllyRecord2}

import scala.collection.immutable.Seq
import scala.concurrent.{ExecutionContext, Future}


// TODO create collection that is optimised for ordered sequence and seqNr
object AllyCassandra {

  case class StatementConfig(
    idempotent: Boolean = false,
    consistencyLevel: ConsistencyLevel,
    retryPolicy: RetryPolicy)


  def apply(
    session2: Session,
    schemaConfig: SchemaConfig,
    config: AllyCassandraConfig)(implicit system: ActorSystem, ec: ExecutionContext): AllyDb = {

    implicit val materializer = ActorMaterializer()

    val retries = 3

    val statementConfig = StatementConfig(
      idempotent = true, /*TODO remove from here*/
      consistencyLevel = ConsistencyLevel.ONE,
      retryPolicy = new LoggingRetryPolicy(NextHostRetryPolicy(retries)))

    val keyspace = schemaConfig.keyspace

    val journalName = s"${ keyspace.name }.${ schemaConfig.journalName }"

    val metadataName = s"${ keyspace.name }.${ schemaConfig.metadataName }"

    val futureUnit = Future.successful(())

    case class PreparedStatements(
      insert: Statements.InsertRecord.Type,
      lastStatement: Statements.Last.Type,
      listStatement: Statements.List.Type)

    def createKeyspace() = {
      // TODO make sure two parallel instances does do the same
      val query =
        s"""
      CREATE KEYSPACE IF NOT EXISTS ${ keyspace.name }
      WITH REPLICATION = { 'class' : ${ keyspace.replicationStrategy.asCql } }
    """
      session2.executeAsync(query).asScala()
    }

    def createTable() = {

      val journal = {

        val query =
          s"""
      CREATE TABLE IF NOT EXISTS $journalName (
        id text,
        segment bigint,
        seq_nr bigint,
        timestamp timestamp,
        payload blob,
        tags set<text>,
        partition int,
        offset bigint,
        PRIMARY KEY ((id, segment), seq_nr, timestamp))
              """
        //        WITH gc_grace_seconds =${gcGrace.toSeconds} TODO
        //        AND compaction = ${config.tableCompactionStrategy.asCQL}
        session2.executeAsync(query).asScala()
      }

      val metadata = {

        // TODO use segment_size

        val query =
          s"""
      CREATE TABLE IF NOT EXISTS $metadataName(
        id text PRIMARY KEY,
        topic text,
        segment_size bigint,
        deleted_to bigint,
        properties map<text,text>,
        created timestamp,
        updated timestamp)
      """
        session2.executeAsync(query).asScala()
      }

      for {
        _ <- journal
        _ <- metadata
      } yield {

      }
    }


    def preparedStatements() = {

      val prepareAndExecute = new Statements.PrepareAndExecute {

        def prepare(query: String) = {
          session2.prepareAsync(query).asScala()
        }

        def execute(statement: BoundStatement) = {
          val statementFinal = statement.set(statementConfig)
          session2.executeAsync(statementFinal).asScala()
        }
      }

      val insertStatement = Statements.InsertRecord(journalName, session2.prepareAsync(_: String).asScala())
      val lastStatement = Statements.Last(journalName, prepareAndExecute)
      val listStatement = Statements.List(journalName, prepareAndExecute)
      for {
        insertStatement <- insertStatement
        lastStatement <- lastStatement
        listStatement <- listStatement
      } yield {
        PreparedStatements(insertStatement, lastStatement, listStatement)
      }
    }

    val sessionAndPreparedStatements = for {
      _ <- if (keyspace.autoCreate) createKeyspace() else futureUnit
      _ <- if (schemaConfig.autoCreate) createTable() else futureUnit
      preparedStatements <- preparedStatements()
    } yield {
      (session2, preparedStatements)
    }

    // TODO read from metadata
    val segmentSize = config.segmentSize


    new AllyDb {

      def save(records: Seq[AllyRecord]): Future[Unit] = {

        if (records.isEmpty) futureUnit
        else {
          val segment = Segment(records.head.seqNr, config.segmentSize)

          def statement(prepared: PreparedStatements) = {

            val statements = for {
              record <- records
            } yield {
              prepared.insert(record, segment)
            }

            if (statements.size == 1) {
              statements.head
            } else {
              val batch = new BatchStatement()
              statements.foldLeft(batch) { _ add _ }
            }
          }

          for {
            (session, prepared) <- sessionAndPreparedStatements
            statementFinal = statement(prepared).set(statementConfig)
            _ <- session.executeAsync(statementFinal).asScala()
          } yield {

          }
        }
      }

      def last(id: Id, from: SeqNr): Future[Option[AllyRecord2]] = {
        println(s"AllyCassandra last id: $id, from: $from")

        def last(statement: Statements.Last.Type) = {


          def recur(from: SeqNr, prev: Option[(Segment, AllyRecord2)]): Future[Option[AllyRecord2]] = {
            // println(s"AllyCassandra.last.recur id: $id, segment: $segment")

            def record = prev.map { case (_, record) => record }

            // TODO use deletedTo
            val segment = Segment(from, segmentSize)
            if (prev.exists { case (segmentPrev, _) => segmentPrev == segment }) {
              Future.successful(record)
            } else {
              for {
                result <- statement(id, from, segment)
                result <- result match {
                  case None         => Future.successful(record)
                  case Some(result) =>
                    val segmentAndRecord = (segment, result)
                    recur(from.next, Some(segmentAndRecord))
                }
              } yield {
                result
              }
            }
          }

          recur(from, None)
        }

        for {
          (session, preparedStatements) <- sessionAndPreparedStatements
          result <- last(preparedStatements.lastStatement)
        } yield {
          result
        }
      }

      def list(id: Id, range: SeqRange): Future[Seq[AllyRecord]] = {

        println(s"AllyCassandra list id: $id, range: $range")

        def list(statement: Statements.List.Type) = {
          val state = (range.from, Option.empty[Segment])
          val source = Source.unfoldAsync(state) { case (from, prev) =>
            // TODO use deletedTo
            val segment = Segment(from, segmentSize)
            if ((range contains from) && !(prev contains segment)) {
              for {
                records <- statement(id, range, segment)
              } yield {
                if (records.isEmpty) {
                  None
                } else {
                  val last = records.last
                  val from = last.seqNr.next
                  val state = (from, Some(segment))
                  val result = (state, records)
                  Some(result)
                }
              }
            } else {
              Future.successful(None) // todo make val
            }
          }

          source
            .mapConcat(identity)
            .runWith(Sink.seq)
        }

        // TODO use partition
        for {
          (session, preparedStatements) <- sessionAndPreparedStatements
          result <- list(preparedStatements.listStatement)
        } yield {


          /*if(id == "p-17") {
            val query =
              s"SELECT seq_nr, segment, offset FROM $journalName WHERE id = ? ALLOW FILTERING"

            val prepared = session.prepare(query)
            val bound = prepared.bind(id)
          val result = session.execute(bound)
            import scala.collection.JavaConverters._
            result.all().asScala.foreach { row =>
              val seqNr = row.getLong("seq_nr")
              val segment = row.getLong("segment")
              val offset = row.getLong("offset")
              println(s"### id: $id, seqNr: $seqNr, segment: $segment, offset: $offset")
            }
          }*/

          println(s"AllyCassandra $id: ${ result.map { _.seqNr }.mkString(",") }")
          result
        }
      }
    }
  }


  implicit class StatementOps(val self: Statement) extends AnyVal {

    def set(statementConfig: StatementConfig): Statement = {
      self
        .setIdempotent(statementConfig.idempotent)
        .setConsistencyLevel(statementConfig.consistencyLevel)
        .setRetryPolicy(statementConfig.retryPolicy)
    }
  }

}

