package com.evolutiongaming.kafka.journal.snapshot.cassandra

import cats.syntax.all._
import cats.{Monad, MonadThrow}
import com.datastax.driver.core.Row
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.kafka.journal.cassandra.CassandraConsistencyConfig
import com.evolutiongaming.kafka.journal.eventual.EventualPayloadAndType
import com.evolutiongaming.kafka.journal.eventual.cassandra.CassandraHelper._
import com.evolutiongaming.kafka.journal.eventual.cassandra.CassandraSession
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
      consistencyConfig: CassandraConsistencyConfig.Write,
      useLWT: Boolean
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
           |${if (useLWT) "IF NOT EXISTS" else ""}
           |""".stripMargin

      for {
        prepared <- query.prepare
      } yield { (key, bufferNr, snapshot) =>
        def statementOf(record: SnapshotRecord[EventualPayloadAndType]) = {
          val snapshot = record.snapshot
          val payloadType = snapshot.payload.payloadType
          val (txt, bin) = snapshot.payload.payload.some.separate

          prepared
            .bind()
            .encode(key)
            .encode(bufferNr)
            .encode(snapshot.seqNr)
            .encode("timestamp", record.timestamp)
            .encodeSome(record.origin)
            .encodeSome(record.version)
            .encode("payload_type", payloadType)
            .encodeSome("payload_txt", txt)
            .encodeSome("payload_bin", bin)
            .setConsistencyLevel(consistencyConfig.value)
        }

        val statement = statementOf(snapshot)
        val row = statement.first

        if (useLWT) {
          row.map(_.fold(false)(_.wasApplied))
        } else {
          row.as(true)
        }
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
      consistencyConfig: CassandraConsistencyConfig.Write,
      useLWT: Boolean
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
           |${if (useLWT) "IF seq_nr = :delete_seq_nr" else ""}
           |""".stripMargin

      for {
        prepared <- query.prepare
      } yield { (key, bufferNr, insertSnapshot, deleteSnapshot) =>
        def statementOf(record: SnapshotRecord[EventualPayloadAndType]) = {
          val snapshot = record.snapshot
          val payloadType = snapshot.payload.payloadType
          val (txt, bin) = snapshot.payload.payload.some.separate

          prepared
            .bind()
            .encode(key)
            .encode(bufferNr)
            .encode("insert_seq_nr", snapshot.seqNr)
            .encodeSome("delete_seq_nr", Option.when(useLWT)(deleteSnapshot))
            .encode("timestamp", record.timestamp)
            .encodeSome(record.origin)
            .encodeSome(record.version)
            .encode("payload_type", payloadType)
            .encodeSome("payload_txt", txt)
            .encodeSome("payload_bin", bin)
            .setConsistencyLevel(consistencyConfig.value)
        }

        val statement = statementOf(insertSnapshot)
        val row = statement.first

        if (useLWT) {
          row.map(_.fold(false)(_.wasApplied))
        } else {
          row.as(true)
        }
      }
    }
  }

  trait SelectMetadata[F[_]] {
    def apply(key: Key): F[Map[BufferNr, (SeqNr, Instant)]]
  }

  object SelectMetadata {

    def of[F[_]: MonadThrow: CassandraSession](
      name: TableName,
      consistencyConfig: CassandraConsistencyConfig.Read,
      useLWT: Boolean
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
        _ <- MonadThrow[F].raiseWhen(useLWT && !consistencyConfig.value.isSerial) {
          new IllegalArgumentException("consistencyConfig should be set to SERIAL or LOCAL_SERIAL when useLWT = true")
        }
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

    def of[F[_]: MonadThrow: CassandraSession](
      name: TableName,
      consistencyConfig: CassandraConsistencyConfig.Read,
      useLWT: Boolean
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
        _ <- MonadThrow[F].raiseWhen(useLWT && !consistencyConfig.value.isSerial) {
          new IllegalArgumentException("consistencyConfig should be set to SERIAL or LOCAL_SERIAL when useLWT = true")
        }
        prepared <- query.prepare
      } yield { (key, bufferNr) =>
        def readPayload(row: Row): EventualPayloadAndType = {
          val payloadType = row.decode[PayloadType]("payload_type")
          val payloadTxt = row.decode[Option[String]]("payload_txt")
          val payloadBin = row.decode[Option[ByteVector]]("payload_bin") getOrElse ByteVector.empty

          EventualPayloadAndType(payloadTxt.toLeft(payloadBin), payloadType)
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

    def of[F[_]: Monad: CassandraSession](
      name: TableName,
      consistencyConfig: CassandraConsistencyConfig.Write,
      useLWT: Boolean
    ): F[Delete[F]] = {

      val query =
        s"""
           |DELETE FROM ${name.toCql}
           |WHERE id = ?
           |AND topic = ?
           |AND buffer_idx = ?
           |${if (useLWT) "IF EXISTS" else ""}
           |""".stripMargin

      for {
        prepared <- query.prepare
      } yield { (key, bufferNr) =>
        val row = prepared
          .bind()
          .encode(key)
          .encode(bufferNr)
          .setConsistencyLevel(consistencyConfig.value)
          .first

        if (useLWT) {
          row.map(_.fold(false)(_.wasApplied))
        } else {
          row.as(true)
        }
      }
    }
  }

}
