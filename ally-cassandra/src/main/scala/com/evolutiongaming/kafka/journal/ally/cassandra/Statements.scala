package com.evolutiongaming.kafka.journal.ally.cassandra

import java.lang.{Long => LongJ}
import java.nio.ByteBuffer
import java.util.Date

import com.datastax.driver.core._
import com.evolutiongaming.concurrent.CurrentThreadExecutionContext
import com.evolutiongaming.kafka.journal.Alias.Id
import com.evolutiongaming.kafka.journal.ally.{AllyRecord, AllyRecord2, PartitionOffset}

import scala.collection.JavaConverters._
import scala.concurrent.Future

object Statements {

  object InsertRecord {
    type Type = AllyRecord => BoundStatement

    // TODO fast future
    // TODO create Prepare -> Run function
    def apply(name: String, prepare: String => Future[PreparedStatement]): Future[Type] = {
      implicit val ec = CurrentThreadExecutionContext // TODO remove
      val query =
        s"INSERT INTO $name (id, partition, seq_nr, timestamp, payload, tags, kafka_partition, kafka_offset) " +
          s"VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
      for {
        statement <- prepare(query)
      } yield {
        record: AllyRecord => {
          val partition: Long = 0

          statement
            .bind()
            .setString("id", record.id)
            .setLong("partition", partition)
            .setLong("seq_nr", record.seqNr)
            .setTimestamp("timestamp", new Date(record.timestamp))
            .setBytes("payload", ByteBuffer.wrap(record.payload))
            .setSet("tags", record.tags.asJava, classOf[String])
            .setInt("kafka_partition", record.partitionOffset.partition)
            .setLong("kafka_offset", record.partitionOffset.offset)
        }
      }
    }
  }


  trait PrepareAndExecute {
    def prepare(query: String): Future[PreparedStatement]
    def execute(statement: BoundStatement): Future[ResultSet]
  }

  object PrepareAndExecute {
    def apply(): PrepareAndExecute = ???
  }


  object Last {
    type Type = (Id, Long) => Future[Option[AllyRecord2]]

    // TODO fast future
    // TODO create Prepare -> Run function
    def apply(name: String, prepareAndExecute: PrepareAndExecute): Future[Type] = {
      implicit val ec = CurrentThreadExecutionContext // TODO remove
      val query =
        s"""
     SELECT seq_nr, kafka_partition, kafka_offset FROM $name WHERE
       id = ? AND
       partition = ?
       ORDER BY seq_nr
       DESC LIMIT 1
    """

      for {
        statement <- prepareAndExecute.prepare(query)
      } yield {
        (id: Id, partition: Long) =>
          val boundStatement = statement.bind(id, partition: LongJ)
          for {
            result <- prepareAndExecute.execute(boundStatement)
          } yield {
            Option(result.one()) map { row =>
              AllyRecord2(
                seqNr = row.getLong("seq_nr"),
                partitionOffset = PartitionOffset(
                  partition = row.getInt("kafka_partition"),
                  offset = row.getLong("kafka_offset")))
            }
          }
      }
    }
  }


  object List {
    type Type = (Id, Long, SeqNrRange) => Future[Vector[AllyRecord]]

    // TODO fast future
    // TODO create Prepare -> Run function
    def apply(name: String, prepareAndExecute: PrepareAndExecute): Future[Type] = {
      implicit val ec = CurrentThreadExecutionContext // TODO remove
      val query =
        s"""
      SELECT * FROM $name WHERE
        id = ? AND
        partition = ? AND
        seq_nr >= ? AND
        seq_nr <= ?
    """

      for {
        statement <- prepareAndExecute.prepare(query)
      } yield {
        (id: Id, partition: Long, range: SeqNrRange) =>

          val boundStatement = statement
            .bind(id, partition: LongJ, range.from: LongJ, range.to: LongJ)
          for {
            result <- prepareAndExecute.execute(boundStatement)
          } yield {
            // TODO fetch batch by batch
            result.all().asScala.toVector.map { row =>
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
}

