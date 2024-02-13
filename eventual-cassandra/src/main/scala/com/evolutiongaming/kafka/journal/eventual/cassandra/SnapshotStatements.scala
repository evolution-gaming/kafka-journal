package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.Monad
import cats.syntax.all._
import com.datastax.driver.core.Row
import com.evolutiongaming.catshelper.ToTry
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.kafka.journal.eventual.EventualPayloadAndType
import com.evolutiongaming.kafka.journal.eventual.cassandra.CassandraHelper._
import com.evolutiongaming.kafka.journal.eventual.cassandra.EventualCassandraConfig.ConsistencyConfig
import com.evolutiongaming.scassandra.syntax._
import com.evolutiongaming.scassandra.{DecodeByName, EncodeByName, TableName}
import scodec.bits.ByteVector

import java.time.Instant
import scala.util.Try

object SnapshotStatements {

  def createTable(name: TableName): String = {
    s"""
       |CREATE TABLE IF NOT EXISTS ${ name.toCql } (
       |id TEXT,
       |topic TEXT,
       |segment BIGINT,
       |buffer_nr INT,
       |seq_nr BIGINT,
       |partition INT,
       |offset BIGINT,
       |timestamp TIMESTAMP,
       |origin TEXT,
       |version TEXT,
       |metadata TEXT,
       |payload_type TEXT,
       |payload_txt TEXT,
       |payload_bin BLOB,
       |status TEXT
       |PRIMARY KEY ((id, topic, segment), buffer_nr))
       |""".stripMargin
  }

  trait InsertRecord[F[_]] {
    def apply(key: Key, segment: SegmentNr, bufferNr: BufferNr, snapshot: SnapshotRecord[EventualPayloadAndType]): F[Unit]
  }

  object InsertRecord {

    def of[F[_] : Monad : CassandraSession : ToTry : JsonCodec.Encode](
      name: TableName,
      consistencyConfig: ConsistencyConfig.Write
    ): F[InsertRecord[F]] = {

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
           |buffer_nr,
           |seq_nr,
           |partition,
           |offset,
           |timestamp,
           |origin,
           |version,
           |payload_type,
           |payload_txt,
           |payload_bin,
           |metadata,
           |status)
           |VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           |""".stripMargin

      for {
        prepared <- query.prepare
      } yield {
        (key: Key, segment: SegmentNr, bufferNr: BufferNr, snapshot: SnapshotRecord[EventualPayloadAndType]) =>

          def statementOf(record: SnapshotRecord[EventualPayloadAndType]) = {
            val snapshot = record.snapshot
            val (payloadType, txt, bin) = snapshot.payload.map { payloadAndType =>
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
              .encode(bufferNr)
              .encode(snapshot.seqNr)
              .encode(record.partitionOffset)
              .encode("timestamp", record.timestamp)
              .encodeSome(record.origin)
              .encodeSome(record.version)
              .encodeSome("payload_type", payloadType)
              .encodeSome("payload_txt", txt)
              .encodeSome("payload_bin", bin)
              .encode("metadata", record.metadata)(encodeByNameRecordMetadata)
              .encode(record.status)
              .setConsistencyLevel(consistencyConfig.value)
          }

          val statement = statementOf(snapshot)
          statement.setConsistencyLevel(consistencyConfig.value).first.void
      }
    }
  }


  trait SelectRecord[F[_]] {

    def apply(key: Key, segment: SegmentNr, bufferNr: BufferNr): F[Option[SnapshotRecord[EventualPayloadAndType]]]
  }

  object SelectRecord {

    def of[F[_] : Monad : CassandraSession : ToTry : JsonCodec.Decode](
      name: TableName,
      consistencyConfig: ConsistencyConfig.Read): F[SelectRecord[F]] = {

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
           |payload_type,
           |payload_txt,
           |payload_bin,
           |metadata,
           |status FROM ${ name.toCql }
           |WHERE id = ?
           |AND topic = ?
           |AND segment = ?
           |AND buffer_nr = ?
           |""".stripMargin

      for {
        prepared <- query.prepare
      } yield {
        new SelectRecord[F] {

          def apply(key: Key, segment: SegmentNr, bufferNr: BufferNr) = {

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
              .encodeAt(3, bufferNr)
              .setConsistencyLevel(consistencyConfig.value)

            val rows = for {
              row <- bound.execute
            } yield {
              val partitionOffset = row.decode[PartitionOffset]

              val payload = readPayload(row)

              val seqNr = row.decode[SeqNr]
              val snapshot = Snapshot(
                seqNr = seqNr,
                payload = payload)

              val metadata = row.decode[Option[RecordMetadata]]("metadata") getOrElse RecordMetadata.empty

              val status = row.decode[SnapshotStatus]

              SnapshotRecord(
                snapshot = snapshot,
                timestamp = row.decode[Instant]("timestamp"),
                origin = row.decode[Option[Origin]],
                version = row.decode[Option[Version]],
                partitionOffset = partitionOffset,
                metadata = metadata,
                status = status)
            }

            rows.first
          }
        }
      }
    }
  }


  trait UpdateStatus[F[_]] {

    def apply(key: Key, segmentNr: SegmentNr, bufferNr: BufferNr, status: SnapshotStatus):  F[Unit]
  }

  object UpdateStatus {

    def of[F[_] : Monad : CassandraSession](
      name: TableName,
      consistencyConfig: ConsistencyConfig.Write
    ): F[UpdateStatus[F]] = {

      val query =
        s"""
           |UPDATE ${ name.toCql }
           |SET status = ?
           |WHERE id = ?
           |AND topic = ?
           |AND segment = ?
           |AND buffer_nr = ?
           |""".stripMargin

      for {
        prepared <- query.prepare
      } yield {
        (key: Key, segmentNr: SegmentNr, bufferNr: BufferNr, status: SnapshotStatus) =>
          prepared
            .bind()
            .encode(status)
            .encode(key)
            .encode(segmentNr)
            .encode(bufferNr)
            .setConsistencyLevel(consistencyConfig.value)
            .first
            .void
      }
    }
  }

}

