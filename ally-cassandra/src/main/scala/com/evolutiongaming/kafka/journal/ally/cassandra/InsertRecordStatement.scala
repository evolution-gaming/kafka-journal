package com.evolutiongaming.kafka.journal.ally.cassandra

import java.nio.ByteBuffer
import java.util.Date

import com.datastax.driver.core.{BoundStatement, PreparedStatement}
import com.evolutiongaming.concurrent.CurrentThreadExecutionContext
import com.evolutiongaming.kafka.journal.ally.AllyRecord

import scala.collection.JavaConverters._
import scala.concurrent.Future

object InsertRecordStatement {
  type Type = AllyRecord => BoundStatement

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

