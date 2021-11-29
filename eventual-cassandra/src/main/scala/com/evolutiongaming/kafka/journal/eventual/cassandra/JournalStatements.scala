package com.evolutiongaming.kafka.journal.eventual.cassandra

import java.time.Instant
import cats.Monad
import cats.data.{NonEmptyList => Nel}
import cats.syntax.all._
import com.datastax.driver.core.{BatchStatement, Row}
import com.evolutiongaming.catshelper.ToTry
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.kafka.journal.eventual.EventualPayloadAndType
import com.evolutiongaming.kafka.journal.eventual.cassandra.CassandraHelper._
import com.evolutiongaming.kafka.journal.eventual.cassandra.EventualCassandraConfig.ConsistencyConfig
import com.evolutiongaming.kafka.journal.eventual.cassandra.HeadersHelper._
import com.evolutiongaming.scassandra.{DecodeByName, EncodeByName, TableName}
import com.evolutiongaming.scassandra.syntax._
import com.evolutiongaming.sstream.Stream
import scodec.bits.ByteVector

import scala.util.Try

object JournalStatements {

  def createTable(name: TableName): String = {
    s"""
       |CREATE TABLE IF NOT EXISTS ${ name.toCql } (
       |id TEXT,
       |topic TEXT,
       |segment BIGINT,
       |seq_nr BIGINT,
       |partition INT,
       |offset BIGINT,
       |timestamp TIMESTAMP,
       |origin TEXT,
       |version TEXT,
       |tags SET<TEXT>,
       |metadata TEXT,
       |payload_type TEXT,
       |payload_txt TEXT,
       |payload_bin BLOB,
       |headers MAP<TEXT, TEXT>,
       |PRIMARY KEY ((id, topic, segment), seq_nr, timestamp))
       |""".stripMargin
  }


  def addHeaders(table: TableName): String = {
    s"ALTER TABLE ${ table.toCql } ADD headers map<text, text>"
  }

  def addVersion(table: TableName): String = {
    s"ALTER TABLE ${ table.toCql } ADD version TEXT"
  }

  trait InsertRecords[F[_]] {
    def apply(key: Key, segment: SegmentNr, events: Nel[EventRecord[EventualPayloadAndType]]): F[Unit]
  }

  object InsertRecords {

    def of[F[_] : Monad : CassandraSession : ToTry : JsonCodec.Encode](name: TableName)(
      implicit consistencyConfig: ConsistencyConfig.Write
    ): F[InsertRecords[F]] = {

      implicit val encodeTry: JsonCodec.Encode[Try] = JsonCodec.Encode.summon[F].mapK(ToTry.functionK)

      implicit val encodeByNameByteVector: EncodeByName[ByteVector] = EncodeByName[Array[Byte]]
        .contramap { _.toArray }

      val encodeByNameRecordMetadata = EncodeByName[RecordMetadata]

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
           |version,
           |tags,
           |payload_type,
           |payload_txt,
           |payload_bin,
           |metadata,
           |headers)
           |VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           |""".stripMargin

      for {
        prepared <- query.prepare
      } yield {
        (key: Key, segment: SegmentNr, events: Nel[EventRecord[EventualPayloadAndType]]) =>

          def statementOf(record: EventRecord[EventualPayloadAndType]) = {
            val event = record.event
            val (payloadType, txt, bin) = event.payload.map { payloadAndType =>
              val (text, bytes) = payloadAndType.payload.fold(
                str   => (str.some, none[ByteVector]),
                bytes => (none[String], bytes.some)
              )
              (payloadAndType.payloadType.some, text, bytes)
            } getOrElse {
              (None, None, None)
            }

            prepared
              .bind()
              .encode(key)
              .encode(segment)
              .encode(event.seqNr)
              .encode(record.partitionOffset)
              .encode("timestamp", record.timestamp)
              .encodeSome(record.origin)
              .encodeSome(record.version)
              .encode("tags", event.tags)
              .encodeSome("payload_type", payloadType)
              .encodeSome("payload_txt", txt)
              .encodeSome("payload_bin", bin)
              .encode("metadata", record.metadata)(encodeByNameRecordMetadata)
              .encode(record.headers)
              .setConsistencyLevel(consistencyConfig.value)
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
          statement.first.void
      }
    }
  }


  trait SelectRecords[F[_]] {

    def apply(key: Key, segment: SegmentNr, range: SeqRange): Stream[F, EventRecord[EventualPayloadAndType]]
  }

  object SelectRecords {

    def of[F[_] : Monad : CassandraSession : ToTry : JsonCodec.Decode](name: TableName)(
      implicit consistencyConfig: ConsistencyConfig.Read
    ): F[SelectRecords[F]] = {

      implicit val encodeTry: JsonCodec.Decode[Try] = JsonCodec.Decode.summon[F].mapK(ToTry.functionK)
      implicit val decodeByNameByteVector: DecodeByName[ByteVector] = DecodeByName[Array[Byte]]
        .map { a => ByteVector.view(a) }

      val query =
        s"""
           |SELECT
           |seq_nr,
           |partition,
           |offset,
           |timestamp,
           |origin,
           |version,
           |tags,
           |payload_type,
           |payload_txt,
           |payload_bin,
           |metadata,
           |headers FROM ${ name.toCql }
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

            def readPayload(row: Row): Option[EventualPayloadAndType] = {
              val payloadType = row.decode[Option[PayloadType]]("payload_type")
              val payloadTxt = row.decode[Option[String]]("payload_txt")
              val payloadBin = row.decode[Option[ByteVector]]("payload_bin") getOrElse ByteVector.empty

              payloadType
                .map(EventualPayloadAndType(payloadTxt.toLeft(payloadBin), _))
            }

            val bound = prepared
              .bind()
              .encode(key)
              .encode(segment)
              .encodeAt(3, range.from)
              .encodeAt(4, range.to)
              .setConsistencyLevel(consistencyConfig.value)

            for {
              row <- bound.execute
            } yield {
              val partitionOffset = row.decode[PartitionOffset]

              val payload = readPayload(row)

              val seqNr = row.decode[SeqNr]
              val event = Event(
                seqNr = seqNr,
                tags = row.decode[Tags]("tags"),
                payload = payload)

              val metadata = row.decode[Option[RecordMetadata]]("metadata") getOrElse RecordMetadata.empty

              val headers = row.decode[Headers]

              EventRecord(
                event = event,
                timestamp = row.decode[Instant]("timestamp"),
                origin = row.decode[Option[Origin]],
                version = row.decode[Option[Version]],
                partitionOffset = partitionOffset,
                metadata = metadata,
                headers = headers)
            }
          }
        }
      }
    }
  }


  trait DeleteTo[F[_]] {

    def apply(key: Key, segmentNr: SegmentNr, seqNr: SeqNr):  F[Unit]
  }

  object DeleteTo {

    def of[F[_] : Monad : CassandraSession](name: TableName)(
      implicit consistencyConfig: ConsistencyConfig.Write
    ): F[DeleteTo[F]] = {

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
        (key: Key, segmentNr: SegmentNr, seqNr: SeqNr) =>
          prepared
            .bind()
            .encode(key)
            .encode(segmentNr)
            .encode(seqNr)
            .setConsistencyLevel(consistencyConfig.value)
            .first
            .void
      }
    }
  }


  trait Delete[F[_]] {

    def apply(key: Key, segmentNr: SegmentNr):  F[Unit]
  }

  object Delete {

    def of[F[_] : Monad : CassandraSession](name: TableName)(
      implicit consistencyConfig: ConsistencyConfig.Write
    ): F[Delete[F]] = {

      val query =
        s"""
           |DELETE FROM ${ name.toCql }
           |WHERE id = ?
           |AND topic = ?
           |AND segment = ?
           |""".stripMargin

      for {
        prepared <- query.prepare
      } yield {
        (key: Key, segmentNr: SegmentNr) =>
          prepared
            .bind()
            .encode(key)
            .encode(segmentNr)
            .setConsistencyLevel(consistencyConfig.value)
            .first
            .void
      }
    }
  }
}

