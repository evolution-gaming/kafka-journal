package com.evolutiongaming.kafka.journal.ally.cassandra

import com.datastax.driver.core.policies.{LoggingRetryPolicy, RetryPolicy}
import com.datastax.driver.core.{BatchStatement, ConsistencyLevel, Session, Statement}
import com.evolutiongaming.cassandra.Helpers._
import com.evolutiongaming.cassandra.NextHostRetryPolicy
import java.lang.{Long => LongJ}
import com.evolutiongaming.kafka.journal.Alias.Id
import com.evolutiongaming.kafka.journal.ally.{AllyDb, AllyRecord, AllyRecord2, PartitionOffset}

import scala.collection.JavaConverters._
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

    case class PreparedStatements(insertRecord: InsertRecordStatement.Type)


    val sessionAndPreparedStatements = for {
      _ <- {
        if (keyspace.autoCreate) {
          // TODO make sure two parallel instances does do the same
          val query =
            s"""
      CREATE KEYSPACE IF NOT EXISTS ${ keyspace.name }
      WITH REPLICATION = { 'class' : ${ keyspace.replicationStrategy.asCql } }
    """
          session2.executeAsync(query).asScala()
        } else {
          futureUnit
        }
      }

      _ <- {
        if (schemaConfig.autoCreate) {
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
        } else {
          futureUnit
        }
      }
      insertStatement <- InsertRecordStatement(tableName, session2.prepareAsync(_: String).asScala())
    } yield {
      val prepared = PreparedStatements(insertStatement)
      (session2, prepared)
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
        val query = s"""
     SELECT seq_nr, kafka_partition, kafka_offset FROM $tableName WHERE
       id = ? AND
       partition = ?
       ORDER BY seq_nr
       DESC LIMIT 1
   """

        val partition: Long = 0 // TODO

        val statement = session2.prepare(query)
        val boundStatement = statement.bind()
          .setString("id", id)
          .setLong("partition", partition)
          .set(statementConfig)

        val result = session2.execute(boundStatement)
        val record = Option(result.one()) map { row =>
          AllyRecord2(
            seqNr = row.getLong("seq_nr"),
            partitionOffset = PartitionOffset(
              partition = row.getInt("kafka_partition"),
              offset = row.getLong("kafka_offset")))
        }
        Future.successful(record)
      }

      def list(id: Id): Future[Vector[AllyRecord]] = {


        // TODO use partition

        for {
          (session, preparedStatements) <- sessionAndPreparedStatements
        } yield {

          val query =
            s"""
      SELECT * FROM $tableName WHERE
        id = ? AND
        partition = ? AND
        seq_nr >= ? AND
        seq_nr <= ?
    """

          val from: Long = 0
          val to: Long = Long.MaxValue
          val partition: Long = 0 // TODO

          val statement = session.prepare(query)

          val boundStatement = statement
            .bind(id, partition: LongJ, from: LongJ, to: LongJ)
            .set(statementConfig)

          //          boundStatement.setFetchSize() // TODO

          val result = session.execute(boundStatement)

          // TODO fetch batch by batch
          result.all().asScala.toVector.map { row =>
            /*id text,
            partition bigint,
            seq_nr bigint,
            timestamp timestamp,
            payload blob,
            tags set<text>,
            kafka_partition int,
            kafka_offset bigint,*/

            AllyRecord(
              id = row.getString("id"),
              seqNr = row.getLong("seq_nr"),
              timestamp = row.getTimestamp("timestamp").getTime,
              payload = row.getBytes("payload").array(),
              tags = row.getSet("tags", classOf[String]).asScala.toSet,
              partitionOffset = PartitionOffset(
                partition = row.getInt("kafka_partition"),
                offset = row.getLong("kafka_offset")))
          }
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

