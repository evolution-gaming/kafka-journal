package com.evolutiongaming.kafka.journal.eventual.cassandra

import java.time.Instant

import cats.implicits._
import cats.{FlatMap, Monad}
import com.datastax.driver.core.BatchStatement
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.kafka.journal.stream.Stream
import com.evolutiongaming.kafka.journal.eventual.cassandra.CassandraHelper._
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


  type InsertRecords[F[_]] = (Key, SegmentNr, Nel[ReplicatedEvent]) => F[Unit]

  // TODO add statement logging
  object InsertRecords {

    def of[F[_] : FlatMap : CassandraSession](name: TableName): F[InsertRecords[F]] = {
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
           |payload_bin,
           |metadata)
           |VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           |""".stripMargin

      for {
        prepared <- query.prepare
      } yield {
        (key: Key, segment: SegmentNr, events: Nel[ReplicatedEvent]) =>

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
              .encode("metadata", replicated.metadata)
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
          statement.execute.void
      }
    }
  }


  trait SelectRecords[F[_]] {
    def apply(key: Key, segment: SegmentNr, range: SeqRange): stream.Stream[F, ReplicatedEvent]
  }

  object SelectRecords {

    // TODO apply -> of
    def of[F[_] : Monad : CassandraSession](name: TableName): F[SelectRecords[F]] = {

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
           |payload_bin,
           |metadata FROM ${ name.toCql }
           |WHERE id = ?
           |AND topic = ?
           |AND segment = ?
           |AND seq_nr >= ?
           |AND seq_nr <= ?
           |""".stripMargin

      for {
        prepared <- query.prepare
      } yield {
        new SelectRecords[F] {

          def apply(key: Key, segment: SegmentNr, range: SeqRange) = {

            val bound = prepared
              .bind()
              .encode(key)
              .encode(segment)
              .setLong(3, range.from.value) // TODO
              .setLong(4, range.to.value) // TODO
            //              .encodeAt(3, range.from)
            //              .encodeAt(4, range.to)

            for {
              result <- Stream.lift(bound.execute)
              // TODO use Stream.map
              row <- result.stream
            } yield {
              val partitionOffset = row.decode[PartitionOffset]

              val payloadType = row.decode[Option[PayloadType]]("payload_type")
              val payload = payloadType.map {
                case PayloadType.Binary => row.decode[Option[Payload.Binary]]("payload_bin") getOrElse Payload.Binary.Empty
                case PayloadType.Text   => row.decode[Payload.Text]("payload_txt")
                case PayloadType.Json   => row.decode[Payload.Json]("payload_txt")
              }

              val seqNr = row.decode[SeqNr]
              val event = Event(
                seqNr = seqNr,
                tags = row.decode[Tags]("tags"),
                payload = payload)

              val metadata = row.decode[Option[Metadata]]("metadata") getOrElse Metadata.Empty

              ReplicatedEvent(
                event = event,
                timestamp = row.decode[Instant]("timestamp"),
                origin = row.decode[Option[Origin]],
                partitionOffset = partitionOffset,
                metadata = metadata)
            }
          }
        }
      }
    }
  }


  type DeleteRecords[F[_]] = (Key, SegmentNr, SeqNr) => F[Unit]

  object DeleteRecords {

    def of[F[_] : FlatMap : CassandraSession](name: TableName): F[DeleteRecords[F]] = {
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
          bound.execute.void
      }
    }
  }
}

