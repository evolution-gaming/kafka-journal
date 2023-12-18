package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.Monad
import cats.syntax.all._
import com.datastax.driver.core.Row
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.kafka.journal.eventual.EventualPayloadAndType
import com.evolutiongaming.kafka.journal.eventual.cassandra.CassandraHelper._
import com.evolutiongaming.scassandra.syntax._
import com.evolutiongaming.scassandra.{DecodeByName, EncodeByName, TableName}
import scodec.bits.ByteVector

import java.time.Instant

object SnapshotStatements {

  def createTable(name: TableName): String = {
    s"""
       |CREATE TABLE IF NOT EXISTS ${name.toCql} (
       |id TEXT,
       |topic TEXT,
       |buffer_idx INT,
       |seq_nr BIGINT,
       |timestamp TIMESTAMP,
       |origin TEXT,
       |version TEXT,
       |metadata TEXT,
       |payload_type TEXT,
       |payload_txt TEXT,
       |payload_bin BLOB,
       |PRIMARY KEY ((id, topic), buffer_idx))
       |""".stripMargin
  }

  trait InsertRecord[F[_]] {
    def apply(key: Key, bufferNr: BufferNr, snapshot: SnapshotRecord[EventualPayloadAndType]): F[Boolean]
  }

  object InsertRecord {

    def of[F[_]: Monad: CassandraSession](
      name: TableName,
      consistencyConfig: CassandraConsistencyConfig.Write
    ): F[InsertRecord[F]] = {

      implicit val encodeByNameByteVector: EncodeByName[ByteVector] =
        EncodeByName[Array[Byte]].contramap(_.toArray)

      val query =
        s"""
           |INSERT INTO ${name.toCql} (
           |id,
           |topic,
           |buffer_idx,
           |seq_nr,
           |timestamp,
           |origin,
           |version,
           |payload_type,
           |payload_txt,
           |payload_bin,
           |metadata)
           |VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           |IF NOT EXISTS
           |""".stripMargin

      for {
        prepared <- query.prepare
      } yield { (key, bufferNr, snapshot) =>
        def statementOf(record: SnapshotRecord[EventualPayloadAndType]) = {
          val snapshot = record.snapshot
          val payloadType = snapshot.payload.map(_.payloadType)
          val (txt, bin) = snapshot.payload.map(_.payload).separate

          prepared
            .bind()
            .encode(key)
            .encode(bufferNr)
            .encode(snapshot.seqNr)
            .encode("timestamp", record.timestamp)
            .encodeSome(record.origin)
            .encodeSome(record.version)
            .encodeSome("payload_type", payloadType)
            .encodeSome("payload_txt", txt)
            .encodeSome("payload_bin", bin)
            .setConsistencyLevel(consistencyConfig.value)
        }

        val statement = statementOf(snapshot)
        val row = statement.first
        row.map(_.fold(false)(_.wasApplied))
      }
    }
  }

  trait UpdateRecord[F[_]] {
    def apply(
      key: Key,
      bufferNr: BufferNr,
      insertSnapshot: SnapshotRecord[EventualPayloadAndType],
      deleteSnapshot: SeqNr
    ): F[Boolean]
  }

  object UpdateRecord {

    def of[F[_]: Monad: CassandraSession](
      name: TableName,
      consistencyConfig: CassandraConsistencyConfig.Write
    ): F[UpdateRecord[F]] = {

      implicit val encodeByNameByteVector: EncodeByName[ByteVector] =
        EncodeByName[Array[Byte]].contramap(_.toArray)

      val query =
        s"""
           |UPDATE ${name.toCql}
           |SET seq_nr = :insert_seq_nr,
           |timestamp = :timestamp,
           |origin = :origin,
           |version = :version,
           |payload_type = :payload_type,
           |payload_txt = :payload_txt,
           |payload_bin = :payload_bin,
           |metadata = :metadata
           |WHERE id = :id
           |AND topic = :topic
           |AND buffer_idx = :buffer_idx
           |IF seq_nr = :delete_seq_nr
           |""".stripMargin

      for {
        prepared <- query.prepare
      } yield { (key, bufferNr, insertSnapshot, deleteSnapshot) =>
        def statementOf(record: SnapshotRecord[EventualPayloadAndType]) = {
          val snapshot = record.snapshot
          val payloadType = snapshot.payload.map(_.payloadType)
          val (txt, bin) = snapshot.payload.map(_.payload).separate

          prepared
            .bind()
            .encode(key)
            .encode(bufferNr)
            .encode("insert_seq_nr", snapshot.seqNr)
            .encode("delete_seq_nr", deleteSnapshot)
            .encode("timestamp", record.timestamp)
            .encodeSome(record.origin)
            .encodeSome(record.version)
            .encodeSome("payload_type", payloadType)
            .encodeSome("payload_txt", txt)
            .encodeSome("payload_bin", bin)
            .setConsistencyLevel(consistencyConfig.value)
        }

        val statement = statementOf(insertSnapshot)
        val row = statement.first
        row.map(_.fold(false)(_.wasApplied))
      }
    }
  }

  trait SelectMetadata[F[_]] {
    def apply(key: Key): F[Map[BufferNr, (SeqNr, Instant)]]
  }

  object SelectMetadata {

    def of[F[_]: Monad: CassandraSession](
      name: TableName,
      consistencyConfig: CassandraConsistencyConfig.Read
    ): F[SelectMetadata[F]] = {

      val query =
        s"""
           |SELECT
           |buffer_idx,
           |seq_nr,
           |timestamp FROM ${name.toCql}
           |WHERE id = ?
           |AND topic = ?
           |""".stripMargin

      for {
        prepared <- query.prepare
      } yield { key =>
        val bound = prepared
          .bind()
          .encode(key)
          .setConsistencyLevel(consistencyConfig.value)

        val rows = for {
          row <- bound.execute
        } yield {

          val seqNr = row.decode[SeqNr]
          val bufferNr = row.decode[BufferNr]
          val timestamp = row.decode[Instant]("timestamp")

          (bufferNr, (seqNr, timestamp))
        }

        rows.toList.map(_.toMap)
      }
    }
  }

  trait SelectRecord[F[_]] {
    def apply(key: Key, bufferNr: BufferNr): F[Option[SnapshotRecord[EventualPayloadAndType]]]
  }

  object SelectRecord {

    def of[F[_]: Monad: CassandraSession](
      name: TableName,
      consistencyConfig: CassandraConsistencyConfig.Read
    ): F[SelectRecord[F]] = {

      implicit val decodeByNameByteVector: DecodeByName[ByteVector] =
        DecodeByName[Array[Byte]].map(ByteVector.view)

      val query =
        s"""
           |SELECT
           |seq_nr,
           |timestamp,
           |origin,
           |version,
           |payload_type,
           |payload_txt,
           |payload_bin,
           |metadata FROM ${name.toCql}
           |WHERE id = ?
           |AND topic = ?
           |AND buffer_idx = ?
           |""".stripMargin

      for {
        prepared <- query.prepare
      } yield { (key, bufferNr) =>
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
          .encodeAt(2, bufferNr)
          .setConsistencyLevel(consistencyConfig.value)

        val rows = for {
          row <- bound.execute
        } yield {

          val payload = readPayload(row)

          val seqNr = row.decode[SeqNr]
          val snapshot = Snapshot(seqNr = seqNr, payload = payload)

          SnapshotRecord(
            snapshot = snapshot,
            timestamp = row.decode[Instant]("timestamp"),
            origin = row.decode[Option[Origin]],
            version = row.decode[Option[Version]]
          )
        }

        rows.first
      }
    }
  }

  trait Delete[F[_]] {

    def apply(key: Key, bufferNr: BufferNr): F[Boolean]
  }

  object Delete {

    def of[F[_]: Monad: CassandraSession](name: TableName, consistencyConfig: CassandraConsistencyConfig.Write): F[Delete[F]] = {

      val query =
        s"""
           |DELETE FROM ${name.toCql}
           |WHERE id = ?
           |AND topic = ?
           |AND buffer_idx = ?
           |IF EXISTS""".stripMargin

      for {
        prepared <- query.prepare
      } yield { (key, bufferNr) =>
        val row = prepared
          .bind()
          .encode(key)
          .encode(bufferNr)
          .setConsistencyLevel(consistencyConfig.value)
          .first
        row.map(_.fold(false)(_.wasApplied))
      }
    }
  }

}
