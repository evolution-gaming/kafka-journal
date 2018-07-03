package com.evolutiongaming.kafka.journal.ally.cassandra

import java.time.Instant

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import com.datastax.driver.core.policies.{LoggingRetryPolicy, RetryPolicy}
import com.datastax.driver.core.{Metadata => _, _}
import com.evolutiongaming.cassandra.Helpers._
import com.evolutiongaming.cassandra.NextHostRetryPolicy
import com.evolutiongaming.kafka.journal.Alias._
import com.evolutiongaming.kafka.journal.SeqRange
import com.evolutiongaming.kafka.journal.ally.{AllyDb, AllyRecord, AllyRecord2}
import com.evolutiongaming.skafka.Topic

import scala.collection.immutable.Seq
import scala.concurrent.{ExecutionContext, Future}


// TODO create collection that is optimised for ordered sequence and seqNr
object AllyCassandra {

  case class StatementConfig(
    idempotent: Boolean = false,
    consistencyLevel: ConsistencyLevel,
    retryPolicy: RetryPolicy)


  def apply(
    session: Session,
    schemaConfig: SchemaConfig,
    config: AllyCassandraConfig)(implicit system: ActorSystem, ec: ExecutionContext): AllyDb = {

    implicit val materializer = ActorMaterializer()

    val retries = 3

    val statementConfig = StatementConfig(
      idempotent = true, /*TODO remove from here*/
      consistencyLevel = ConsistencyLevel.ONE,
      retryPolicy = new LoggingRetryPolicy(NextHostRetryPolicy(retries)))

    val keyspace = schemaConfig.keyspace

    val journalName = TableName(keyspace = keyspace.name, table = schemaConfig.journalName)
    
    val metadataName = TableName(keyspace = keyspace.name, table = schemaConfig.metadataName)

    val futureUnit = Future.successful(())

    // TODO moveout
    case class PreparedStatements(
      insertRecord: JournalStatement.InsertRecord.Type,
      selectLastRecord: JournalStatement.SelectLastRecord.Type,
      selectRecords: JournalStatement.SelectRecords.Type,
      insertMetadata: MetadataStatement.Insert.Type,
      selectMetadata: MetadataStatement.Select.Type,
      selectSegmentSize: MetadataStatement.SelectSegmentSize.Type)

    def createKeyspace() = {
      // TODO make sure two parallel instances does not do the same
      val query = JournalStatement.createKeyspace(keyspace)
      session.executeAsync(query).asScala()
    }

    def createTable() = {

      val journal = {
        val query = JournalStatement.createTable(journalName)
        session.executeAsync(query).asScala()
      }

      val metadata = {
        val query = MetadataStatement.createTable(metadataName)
        session.executeAsync(query).asScala()
      }

      for {
        _ <- journal
        _ <- metadata
      } yield {
        
      }
    }


    def preparedStatements() = {

      val prepareAndExecute = new PrepareAndExecute {

        def prepare(query: String) = {
          session.prepareAsync(query).asScala()
        }

        def execute(statement: BoundStatement) = {
          val statementConfigured = statement.set(statementConfig)
          val result = session.executeAsync(statementConfigured)
          result.asScala()
        }
      }

      val insertRecord = JournalStatement.InsertRecord(journalName, session.prepareAsync(_: String).asScala())
      val selectLastRecord = JournalStatement.SelectLastRecord(journalName, prepareAndExecute)
      val listRecords = JournalStatement.SelectRecords(journalName, prepareAndExecute)
      val insertMetadata = MetadataStatement.Insert(metadataName, prepareAndExecute)
      val selectMetadata = MetadataStatement.Select(metadataName, prepareAndExecute)
      val selectSegmentSize = MetadataStatement.SelectSegmentSize(metadataName, prepareAndExecute)

      for {
        insertRecord <- insertRecord
        selectLastRecord <- selectLastRecord
        listRecords <- listRecords
        insertMetadata <- insertMetadata
        selectMetadata <- selectMetadata
        selectSegmentSize <- selectSegmentSize
      } yield {
        PreparedStatements(
          insertRecord,
          selectLastRecord,
          listRecords,
          insertMetadata,
          selectMetadata,
          selectSegmentSize)
      }
    }

    val sessionAndPreparedStatements = for {
      _ <- if (keyspace.autoCreate) createKeyspace() else futureUnit
      _ <- if (schemaConfig.autoCreate) createTable() else futureUnit
      preparedStatements <- preparedStatements()
    } yield {
      (session, preparedStatements)
    }

    def segmentSize(id: Id, preparedStatements: PreparedStatements): Future[Int] = {
      val selectSegmentSize = preparedStatements.selectSegmentSize
      for {
        segmentSize <- selectSegmentSize(id)
      } yield {
        segmentSize getOrElse config.segmentSize
      }
    }


    new AllyDb {

      // TODO verify all records have same id
      def save(records: Seq[AllyRecord], topic: Topic): Future[Unit] = {

        //        Thread.sleep(400)

        if (records.isEmpty) futureUnit
        else {

          val head = records.head
          val segment = Segment(head.seqNr, config.segmentSize)
          val id = head.id

          def statement(prepared: PreparedStatements, segmentSize: Int) = {

            val statements = for {
              record <- records
            } yield {
              prepared.insertRecord(record, segment)
            }

            if (statements.size == 1) {
              statements.head
            } else {
              val batch = new BatchStatement()
              statements.foldLeft(batch) { _ add _ }
            }
          }

          def insertMetadata(prepared: PreparedStatements) = {
            // TODO make constant of SeqNr 1
            if (head.seqNr == 1) {

              val timestamp = Instant.now()
              val segmentSize = config.segmentSize
              // TODO is it right to use `currentTime` as timestamp ?
              val metadata = Metadata(id, topic, segmentSize, created = timestamp, updated = timestamp)
              println(s"$id AllyCassandra.metadata: $metadata")
              for {
                _ <- prepared.insertMetadata(metadata)
              } yield {
                segmentSize
              }
            } else {
              segmentSize(id, prepared)
            }
          }

          for {
            (session, prepared) <- sessionAndPreparedStatements
            segmentSize <- insertMetadata(prepared)
            statementFinal = statement(prepared, segmentSize).set(statementConfig)
            _ <- session.executeAsync(statementFinal).asScala()
          } yield {

          }
        }
      }

      def last(id: Id, from: SeqNr): Future[Option[AllyRecord2]] = {
        println(s"$id AllyCassandra.last from: $from")

        def last(statement: JournalStatement.SelectLastRecord.Type, segmentSize: Int) = {

          def recur(from: SeqNr, prev: Option[(Segment, AllyRecord2)]): Future[Option[AllyRecord2]] = {
            // println(s"AllyCassandra.last.recur id: $id, segment: $segment")

            def record = prev.map { case (_, record) => record }

            // TODO use deletedTo
            val segment = Segment(from, segmentSize)
            if (prev.exists { case (segmentPrev, _) => segmentPrev == segment }) {
              Future.successful(record)
            } else {
              for {
                result <- statement(id, segment, from)
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
          segmentSize <- segmentSize(id, preparedStatements)
          result <- last(preparedStatements.selectLastRecord, segmentSize)
        } yield {
          result
        }
      }

      def list(id: Id, range: SeqRange): Future[Seq[AllyRecord]] = {

        println(s"$id AllyCassandra.list range: $range")

        def list(statement: JournalStatement.SelectRecords.Type, segmentSize: Int) = {
          val state = (range.from, Option.empty[Segment])
          val source = Source.unfoldAsync(state) { case (from, prev) =>
            // TODO use deletedTo
            val segment = Segment(from, segmentSize)
            if ((range contains from) && !(prev contains segment)) {
              for {
                records <- statement(id, segment, range)
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
          segmentSize <- segmentSize(id, preparedStatements)
          result <- list(preparedStatements.selectRecords, segmentSize)
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

          println(s"$id AllyCassandra.list ${ result.map { _.seqNr }.mkString(",") }")
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

