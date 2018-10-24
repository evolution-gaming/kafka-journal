package com.evolutiongaming.kafka.journal.eventual.cassandra

import java.lang.{Long => LongJ}
import java.time.Instant

import com.datastax.driver.core.BatchStatement
import com.evolutiongaming.scassandra.CassandraHelper._
import com.evolutiongaming.concurrent.async.Async
import com.evolutiongaming.kafka.journal.FoldWhileHelper._
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.kafka.journal.eventual.cassandra.CassandraHelper._
import com.evolutiongaming.nel.Nel
import play.api.libs.json.Json

import scala.concurrent.ExecutionContext

object JournalStatement {

  // TODO store metadata as json text
  def createTable(name: TableName): String = {
    s"""
       |CREATE TABLE IF NOT EXISTS ${ name.asCql } (
       |id text,
       |topic text,
       |segment bigint,
       |seq_nr bigint,
       |partition int,
       |offset bigint,
       |timestamp timestamp,
       |origin text,
       |tags set<text>,
       |metadata text,
       |payload_type text,
       |payload_txt text,
       |payload_bin blob,
       |PRIMARY KEY ((id, topic, segment), seq_nr, timestamp))
       |""".stripMargin
    //        WITH gc_grace_seconds =${gcGrace.toSeconds} TODO
    //        AND compaction = ${config.tableCompactionStrategy.asCQL}
  }


  // TODO add statement logging
  object InsertRecords {
    type Type = (Key, SegmentNr, Nel[ReplicatedEvent]) => Async[Unit]

    def apply(name: TableName, session: PrepareAndExecute): Async[Type] = {
      val query =
        s"""
           |INSERT INTO ${ name.asCql } (
           |id,
           |topic,
           |segment,
           |seq_nr,
           |partition,
           |offset,
           |timestamp,
           |origin,
           |tags,
           |payload_type,
           |payload_txt,
           |payload_bin)
           |VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           |""".stripMargin

      for {
        prepared <- session.prepare(query)
      } yield {
        (key: Key, segment: SegmentNr, events: Nel[ReplicatedEvent]) =>
          // TODO make up better way for creating queries

          // TODO add metadata field

          def statementOf(replicated: ReplicatedEvent) = {
            val event = replicated.event
            // TODO test this
            val (payloadType, txt, bin) = event.payload.map { payload =>
              val (text, bytes) = payload match {
                case payload: Payload.Binary => (None, Some(payload))
                case payload: Payload.Text   => (Some(payload.value), None)
                case payload: Payload.Json   => (Some(Json.stringify(payload.value)), None)
              }
              val payloadType = payload.payloadType
              (Some(payloadType): Option[PayloadType], text, bytes)
            } getOrElse {
              (None, None, None)
            }

            prepared
              .bind()
              .encode(key)
              .encode(segment)
              .encode(event.seqNr)
              .encode(replicated.partitionOffset)
              .encode("timestamp", replicated.timestamp)
              .encode(replicated.origin)
              .encode("tags", event.tags)
              .encode("payload_type", payloadType)
              .encode("payload_txt", txt)
              .encode("payload_bin", bin)
          }

          val statement = {
            if (events.tail.isEmpty) {
              val statement = statementOf(events.head)
              session.execute(statement)
            } else {
              val statement = events.foldLeft(new BatchStatement()) { (batch, event) =>
                batch.add(statementOf(event))
              }
              session.execute(statement)
            }
          }
          statement.unit
      }
    }
  }


  // TODO rename along with EventualRecord2
  object SelectLastRecord {

    type Type[F[_]] = (Key, SegmentNr, SeqNr) => F[Option[Pointer]]

    def apply(name: TableName, session: PrepareAndExecute): Async[Type[Async]] = {
      val query =
        s"""
           |SELECT seq_nr, partition, offset
           |FROM ${ name.asCql }
           |WHERE id = ?
           |AND topic = ?
           |AND segment = ?
           |AND seq_nr >= ?
           |ORDER BY seq_nr
           |DESC LIMIT 1
           |""".stripMargin

      for {
        prepared <- session.prepare(query)
      } yield {
        (key: Key, segment: SegmentNr, from: SeqNr) =>
          val bound = prepared
            .bind()
            .encode(key)
            .encode(segment)
            .encode(from)
          for {
            result <- session.execute(bound)
          } yield for {
            row <- Option(result.one())
          } yield {
            Pointer(
              seqNr = row.decode[SeqNr],
              partitionOffset = row.decode[PartitionOffset])
          }
      }
    }
  }


  object SelectRecords {

    trait Type {
      def apply[S](key: Key, segment: SegmentNr, range: SeqRange, s: S)(f: Fold[S, ReplicatedEvent]): Async[Switch[S]]
    }

    def apply(name: TableName, session: PrepareAndExecute)(implicit ec: ExecutionContext): Async[Type] = {
      val query =
        s"""
           |SELECT
           |seq_nr,
           |partition,
           |offset,
           |timestamp,
           |origin,
           |tags,
           |payload_type,
           |payload_txt,
           |payload_bin FROM ${ name.asCql }
           |WHERE id = ?
           |AND topic = ?
           |AND segment = ?
           |AND seq_nr >= ?
           |AND seq_nr <= ?
           |""".stripMargin

      for {
        prepared <- session.prepare(query)
      } yield {
        new Type {
          def apply[S](key: Key, segment: SegmentNr, range: SeqRange, s: S)(f: Fold[S, ReplicatedEvent]) = {

            val fetchThreshold = 10 // TODO #49,

            // TODO avoid casting via providing implicit converters
            val bound = prepared
              .bind(key.id, key.topic, segment.value: LongJ, range.from.value: LongJ, range.to.value: LongJ)

            for {
              result <- session.execute(bound)
              result <- result.foldWhile(fetchThreshold, s) { case (s, row) =>
                val partitionOffset = row.decode[PartitionOffset]

                val payloadType = row.decode[Option[PayloadType]]("payload_type")
                val payload = payloadType.map {
                  case PayloadType.Binary => row.decode[Option[Payload.Binary]]("payload_bin") getOrElse Payload.Binary.Empty
                  case PayloadType.Text   => row.decode[Payload.Text]("payload_txt")
                  case PayloadType.Json   => row.decode[Payload.Json]("payload_txt")
                }

                val event = Event(
                  seqNr = row.decode[SeqNr],
                  tags = row.decode[Tags]("tags"),
                  payload = payload)
                val replicated = ReplicatedEvent(
                  event = event,
                  timestamp = row.decode[Instant]("timestamp"),
                  origin = row.decode[Option[Origin]],
                  partitionOffset = partitionOffset)
                f(s, replicated)
              }
            } yield result
          }
        }
      }
    }
  }

  object DeleteRecords {
    type Type = (Key, SegmentNr, SeqNr) => Async[Unit]

    def apply(name: TableName, session: PrepareAndExecute): Async[Type] = {
      val query =
        s"""
           |DELETE FROM ${ name.asCql }
           |WHERE id = ?
           |AND topic = ?
           |AND segment = ?
           |AND seq_nr <= ?
           |""".stripMargin

      for {
        prepared <- session.prepare(query)
      } yield {
        (key: Key, segment: SegmentNr, seqNr: SeqNr) =>
          val bound = prepared
            .bind()
            .encode(key)
            .encode(segment)
            .encode(seqNr)
          session.execute(bound).unit
      }
    }
  }
}

