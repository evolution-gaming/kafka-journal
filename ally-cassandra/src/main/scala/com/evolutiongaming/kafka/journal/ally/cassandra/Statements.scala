package com.evolutiongaming.kafka.journal.ally.cassandra

import java.lang.{Long => LongJ}
import java.nio.ByteBuffer
import java.util.Date

import com.datastax.driver.core._
import com.evolutiongaming.concurrent.CurrentThreadExecutionContext
import com.evolutiongaming.kafka.journal.Alias.{Id, SeqNr}
import com.evolutiongaming.kafka.journal.SeqRange
import com.evolutiongaming.kafka.journal.ally.{AllyRecord, AllyRecord2, PartitionOffset}

import scala.collection.JavaConverters._
import scala.concurrent.Future

object Statements {





  trait PrepareAndExecute {
    def prepare(query: String): Future[PreparedStatement]
    def execute(statement: BoundStatement): Future[ResultSet]
  }
  

  object InsertRecord {
    type Type = (AllyRecord, Segment) => BoundStatement

    // TODO fast future
    // TODO create Prepare -> Run function
    def apply(name: String, prepare: String => Future[PreparedStatement]): Future[Type] = {
      implicit val ec = CurrentThreadExecutionContext // TODO remove
      val query =
        s"""
           |INSERT INTO $name (id, segment, seq_nr, timestamp, payload, tags, partition, offset)
           |VALUES (?, ?, ?, ?, ?, ?, ?, ?)
           |""".stripMargin

      for {
        prepared <- prepare(query)
      } yield {
        (record: AllyRecord, segment: Segment) => {
          // TODO make up better way for creating queries
          prepared
            .bind()
            .setString("id", record.id)
            .setLong("segment", segment.value)
            .setLong("seq_nr", record.seqNr)
            .setTimestamp("timestamp", new Date(record.timestamp))
            .setBytes("payload", ByteBuffer.wrap(record.payload))
            .setSet("tags", record.tags.asJava, classOf[String])
            .setInt("partition", record.partitionOffset.partition)
            .setLong("offset", record.partitionOffset.offset)
        }
      }
    }
  }


  object Last {
    // TODO add from ?
    type Type = (Id, SeqNr, Segment) => Future[Option[AllyRecord2]]

    // TODO fast future
    // TODO create Prepare -> Run function
    def apply(name: String, prepareAndExecute: PrepareAndExecute): Future[Type] = {
      implicit val ec = CurrentThreadExecutionContext // TODO remove
      val query =
        s"""
           |SELECT seq_nr, partition, offset
           |FROM $name
           |WHERE id = ?
           |AND segment = ?
           |AND seq_nr >= ?
           |ORDER BY seq_nr
           |DESC LIMIT 1
           |""".stripMargin

      for {
        prepared <- prepareAndExecute.prepare(query)
      } yield {
        (id: Id, from: SeqNr, segment: Segment) =>
          val bound = prepared.bind(id, segment.value: LongJ, from: LongJ)
          for {
            result <- prepareAndExecute.execute(bound)
          } yield {
            val row = Option(result.one())
            row map { row =>
              val partitionOffset = PartitionOffset(
                partition = row.getInt("partition"),
                offset = row.getLong("offset"))
              AllyRecord2(
                seqNr = row.getLong("seq_nr"),
                partitionOffset = partitionOffset)
            }
          }
      }
    }
  }


  object List {
    type Type = (Id, SeqRange, Segment) => Future[Vector[AllyRecord]]

    // TODO fast future
    // TODO create Prepare -> Run function
    def apply(name: String, prepareAndExecute: PrepareAndExecute): Future[Type] = {
      implicit val ec = CurrentThreadExecutionContext // TODO remove
      val query =
        s"""
           |SELECT * FROM $name
           |WHERE id = ?
           |AND segment = ?
           |AND seq_nr >= ?
           |AND seq_nr <= ?
           |""".stripMargin

      for {
        prepared <- prepareAndExecute.prepare(query)
      } yield {
        (id: Id, range: SeqRange, segment: Segment) =>


          // TODO avoid casting via providing implicit converters
          val bound = prepared.bind(id, segment.value: LongJ, range.from: LongJ, range.to: LongJ)
          for {
            result <- prepareAndExecute.execute(bound)
          } yield {
            // TODO fetch batch by batch
            val xs = result.all().asScala.toVector.map { row =>
              AllyRecord(
                id = row.getString("id"),
                seqNr = row.getLong("seq_nr"),
                timestamp = row.getTimestamp("timestamp").getTime,
                payload = row.getBytes("payload").array(),
                tags = row.getSet("tags", classOf[String]).asScala.toSet,
                partitionOffset = PartitionOffset(
                  partition = row.getInt("partition"),
                  offset = row.getLong("offset")))
            }

            //            if(id == "p-17") {
            //              println(s"Statements.list id: $id, segment: $segment, range: $range, result ${xs.map{_.seqNr}}")
            //            }

            xs
          }
      }
    }
  }
}

