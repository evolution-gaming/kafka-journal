package com.evolutiongaming.kafka.journal.eventual.cassandra

import java.lang.{Long => LongJ}
import java.time.Instant

import com.datastax.driver.core.{Metadata => _, _}
import com.evolutiongaming.concurrent.CurrentThreadExecutionContext
import com.evolutiongaming.kafka.journal.Alias.{Id, SeqNr, Tags}
import com.evolutiongaming.kafka.journal.SeqRange
import com.evolutiongaming.kafka.journal.eventual.cassandra.CassandraHelper._
import com.evolutiongaming.kafka.journal.eventual.{EventualRecord, Pointer, PartitionOffset}
import com.evolutiongaming.skafka.{Bytes, Offset, Partition}

import scala.collection.JavaConverters._
import scala.concurrent.Future

object JournalStatement {

  def createKeyspace(keyspace: SchemaConfig.Keyspace): String = {
    // TODO make sure two parallel instances does not do the same
    s"""
       |CREATE KEYSPACE IF NOT EXISTS ${ keyspace.name }
       |WITH REPLICATION = { 'class' : ${ keyspace.replicationStrategy.asCql } }
       |""".stripMargin
  }


  def createTable(name: TableName): String = {
    s"""
       |CREATE TABLE IF NOT EXISTS ${ name.asCql } (
       |id text,
       |segment bigint,
       |seq_nr bigint,
       |timestamp timestamp,
       |payload blob,
       |tags set<text>,
       |partition int,
       |offset bigint,
       |PRIMARY KEY ((id, segment), seq_nr, timestamp))
       |""".stripMargin
    //        WITH gc_grace_seconds =${gcGrace.toSeconds} TODO
    //        AND compaction = ${config.tableCompactionStrategy.asCQL}
  }


  object InsertRecord {
    type Type = (EventualRecord, Segment) => BoundStatement

    // TODO fast future
    // TODO create Prepare -> Run function
    def apply(name: TableName, prepare: String => Future[PreparedStatement]): Future[Type] = {
      implicit val ec = CurrentThreadExecutionContext // TODO remove
      val query =
        s"""
           |INSERT INTO ${ name.asCql } (id, segment, seq_nr, timestamp, payload, tags, partition, offset)
           |VALUES (?, ?, ?, ?, ?, ?, ?, ?)
           |""".stripMargin

      for {
        prepared <- prepare(query)
      } yield {
        (record: EventualRecord, segment: Segment) => {
          // TODO make up better way for creating queries
          prepared
            .bind()
            .encode("id", record.id)
            .encode("segment", segment.value)
            .encode("seq_nr", record.seqNr)
            .encode("timestamp", record.timestamp)
            .encode("payload", record.payload)
            .encode("tags", record.tags)
            .encode("partition", record.partitionOffset.partition)
            .encode("offset", record.partitionOffset.offset)
        }
      }
    }
  }


  // TODO rename along with EventualRecord2
  object SelectLastRecord {
    // TODO add from ?
    type Type = (Id, Segment, SeqNr) => Future[Option[Pointer]]

    // TODO fast future
    // TODO create Prepare -> Run function
    def apply(name: TableName, session: PrepareAndExecute): Future[Type] = {
      implicit val ec = CurrentThreadExecutionContext // TODO remove
      val query =
        s"""
           |SELECT seq_nr, partition, offset
           |FROM ${ name.asCql }
           |WHERE id = ?
           |AND segment = ?
           |AND seq_nr >= ?
           |ORDER BY seq_nr
           |DESC LIMIT 1
           |""".stripMargin

      for {
        prepared <- session.prepare(query)
      } yield {
        (id: Id, segment: Segment, from: SeqNr) =>
          val bound = prepared.bind(id, segment.value: LongJ, from: LongJ)
          for {
            result <- session.execute(bound)
          } yield for {
            row <- Option(result.one())
          } yield {
            val partitionOffset = PartitionOffset(
              partition = row.decode[Partition]("partition"),
              offset = row.decode[Offset]("offset"))
            Pointer(
              seqNr = row.decode[SeqNr]("seq_nr"),
              partitionOffset = partitionOffset)
          }
      }
    }
  }


  object SelectRecords {
    type Type = (Id, Segment, SeqRange) => Future[Vector[EventualRecord]]

    // TODO fast future
    // TODO create Prepare -> Run function
    def apply(name: TableName, session: PrepareAndExecute): Future[Type] = {
      implicit val ec = CurrentThreadExecutionContext // TODO remove
      val query =
        s"""
           |SELECT seq_nr, timestamp, payload, tags, partition, offset FROM ${ name.asCql }
           |WHERE id = ?
           |AND segment = ?
           |AND seq_nr >= ?
           |AND seq_nr <= ?
           |""".stripMargin

      for {
        prepared <- session.prepare(query)
      } yield {
        (id: Id, segment: Segment, range: SeqRange) =>

          // TODO avoid casting via providing implicit converters
          val bound = prepared.bind(id, segment.value: LongJ, range.from: LongJ, range.to: LongJ)
          for {
            result <- session.execute(bound)
          } yield {
            // TODO fetch batch by batch
            result.all().asScala.toVector.map { row =>
              EventualRecord(
                id = id,
                seqNr = row.decode[SeqNr]("seq_nr"),
                timestamp = row.decode[Instant]("timestamp"),
                payload = row.decode[Bytes]("payload"),
                tags = row.decode[Tags]("tags"),
                partitionOffset = PartitionOffset(
                  partition = row.decode[Partition]("partition"),
                  offset = row.decode[Offset]("offset")))
            }
          }
      }
    }
  }
}

