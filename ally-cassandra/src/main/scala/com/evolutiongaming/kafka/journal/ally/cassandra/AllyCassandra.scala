package com.evolutiongaming.kafka.journal.ally.cassandra

import com.datastax.driver.core._
import com.datastax.driver.core.policies.{LoggingRetryPolicy, RetryPolicy}
import com.evolutiongaming.cassandra.Helpers._
import com.evolutiongaming.cassandra.NextHostRetryPolicy
import com.evolutiongaming.kafka.journal.Alias.Id
import com.evolutiongaming.kafka.journal.ally.{AllyDb, AllyRecord, AllyRecord2}

import scala.collection.immutable.Seq
import scala.concurrent.{ExecutionContext, Future}


// TODO create collection that is optimised to ordered sequence and seqNr
object AllyCassandra {

  case class StatementConfig(
    idempotent: Boolean = false,
    consistencyLevel: ConsistencyLevel,
    retryPolicy: RetryPolicy)


  def apply(session2: Session, schemaConfig: SchemaConfig)(implicit ec: ExecutionContext): AllyDb = {

    val retries = 3

    val statementConfig = StatementConfig(
      idempotent = true, /*TODO remove from here*/
      consistencyLevel = ConsistencyLevel.ONE,
      retryPolicy = new LoggingRetryPolicy(NextHostRetryPolicy(retries)))

    val keyspace = schemaConfig.keyspace

    val tableName = s"${ keyspace.name }.${ schemaConfig.table }"

    val futureUnit = Future.successful(())

    case class PreparedStatements(
      insertRecord: Statements.InsertRecord.Type,
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
      val query =
        s"""
      CREATE TABLE IF NOT EXISTS $tableName (
        id text,
        partition bigint,
        seq_nr bigint,
        timestamp timestamp,
        payload blob,
        tags set<text>,
        kafka_partition int,
        kafka_offset bigint,
        PRIMARY KEY ((id, partition), seq_nr, timestamp))
              """
      // TODO
      /*
      * WITH CLUSTERING ORDER BY (sequence_nr DESC) AND gc_grace_seconds =${ snapshotConfig.gcGraceSeconds }
    AND compaction = ${ snapshotConfig.tableCompactionStrategy.asCQL }*/


      //        WITH gc_grace_seconds =${gcGrace.toSeconds} TODO
      //        AND compaction = ${config.tableCompactionStrategy.asCQL}
      session2.executeAsync(query).asScala()
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

      val insertStatement = Statements.InsertRecord(tableName, session2.prepareAsync(_: String).asScala())
      val lastStatement = Statements.Last(tableName, prepareAndExecute)
      val listStatement = Statements.List(tableName, prepareAndExecute)
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

    new AllyDb {

      def save(records: Seq[AllyRecord]): Future[Unit] = {
        if (records.isEmpty) futureUnit
        else {
          def statement(preparedStatements: PreparedStatements) = {
            if (records.size == 1) {
              val record = records.head
              preparedStatements.insertRecord(record)
            } else {
              val batchStatement = new BatchStatement()
              records.foldLeft(batchStatement) { case (batchStatement, record) =>
                val statement = preparedStatements.insertRecord(record)
                batchStatement.add(statement)
              }
            }
          }

          for {
            (session, preparedStatements) <- sessionAndPreparedStatements
            statementFinal = statement(preparedStatements).set(statementConfig)
            _ <- session.executeAsync(statementFinal).asScala()
          } yield {

          }
        }
      }

      def last(id: Id): Future[Option[AllyRecord2]] = {
        val partition: Long = 0 // TODO

        for {
          (session, preparedStatements) <- sessionAndPreparedStatements
          result <- preparedStatements.lastStatement(id, partition)
        } yield {
          result
        }
      }

      def list(id: Id): Future[Vector[AllyRecord]] = {

        val from: Long = 0
        val to: Long = Long.MaxValue
        val partition: Long = 0 // TODO

        val range = SeqNrRange(from = from, to = to)

        // TODO use partition

        for {
          (session, preparedStatements) <- sessionAndPreparedStatements
          result <- preparedStatements.listStatement(id, partition, range)
        } yield {
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

