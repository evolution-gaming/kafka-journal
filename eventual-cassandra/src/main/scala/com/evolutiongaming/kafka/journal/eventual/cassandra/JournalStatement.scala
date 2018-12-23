package com.evolutiongaming.kafka.journal.eventual.cassandra

import java.lang.{Long => LongJ}
import java.time.Instant

import cats.FlatMap
import cats.implicits._
import com.datastax.driver.core.BatchStatement
import com.evolutiongaming.kafka.journal.FoldWhile._
import com.evolutiongaming.kafka.journal.eventual.cassandra.CassandraHelper._
import com.evolutiongaming.kafka.journal.util.CatsHelper._
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.scassandra.TableName
import com.evolutiongaming.scassandra.syntax._
import play.api.libs.json.Json


object JournalStatement {

  // TODO store metadata as json text
  def createTable(name: TableName): String = {
    s"""
       |CREATE TABLE IF NOT EXISTS ${ name.toCql } (
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
    type Type[F[_]] = (Key, SegmentNr, Nel[ReplicatedEvent]) => F[Unit]

    def apply[F[_] : FlatMap : CassandraSession](name: TableName): F[Type[F]] = {
      val query =
        s"""
           |INSERT INTO ${ name.toCql } (
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
        prepared <- query.prepare
      } yield {
        (key: Key, segment: SegmentNr, events: Nel[ReplicatedEvent]) =>

          // TODO use metadata field

          def statementOf(replicated: ReplicatedEvent) = {
            val event = replicated.event
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

            val result = prepared
              .bind()
              .encode(key)
              .encode(segment)
              .encode(event.seqNr)
              .encode(replicated.partitionOffset)
              .encode("timestamp", replicated.timestamp)
              .encodeSome(replicated.origin)
              .encode("tags", event.tags)
              .encodeSome("payload_type", payloadType)
              .encodeSome("payload_txt", txt)
              .encodeSome("payload_bin", bin)
            result
          }

          val statement = {
            if (events.tail.isEmpty) {
              statementOf(events.head)
            } else {
              events.foldLeft(new BatchStatement()) { (batch, event) =>
                batch.add(statementOf(event))
              }
            }
          }
          statement.execute.unit
      }
    }
  }


  object SelectRecords {

    trait Type[F[_]] {
      def apply[S](key: Key, segment: SegmentNr, range: SeqRange, s: S)(f: Fold[S, ReplicatedEvent]): F[Switch[S]]
    }

    def apply[F[_] : IO2 : FromFuture /*TODO REMOVE*/ : CassandraSession](name: TableName): F[Type[F]] = {

      import com.evolutiongaming.kafka.journal.IO2.ops._

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
           |payload_bin FROM ${ name.toCql }
           |WHERE id = ?
           |AND topic = ?
           |AND segment = ?
           |AND seq_nr >= ?
           |AND seq_nr <= ?
           |""".stripMargin

      for {
        prepared <- query.prepare
      } yield {
        new Type[F] {
          def apply[S](key: Key, segment: SegmentNr, range: SeqRange, s: S)(f: Fold[S, ReplicatedEvent]) = {

            val fetchThreshold = 10 // TODO #49,

            // TODO avoid casting via providing implicit converters
            val bound = prepared
              .bind(key.id, key.topic, segment.value: LongJ, range.from.value: LongJ, range.to.value: LongJ)

            for {
              result <- bound.execute
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
    type Type[F[_]] = (Key, SegmentNr, SeqNr) => F[Unit]

    def apply[F[_]: FlatMap: CassandraSession](name: TableName): F[Type[F]] = {
      val query =
        s"""
           |DELETE FROM ${ name.toCql }
           |WHERE id = ?
           |AND topic = ?
           |AND segment = ?
           |AND seq_nr <= ?
           |""".stripMargin

      for {
        prepared <- query.prepare
      } yield {
        (key: Key, segment: SegmentNr, seqNr: SeqNr) =>
          val bound = prepared
            .bind()
            .encode(key)
            .encode(segment)
            .encode(seqNr)
          bound.execute.unit
      }
    }
  }
}

