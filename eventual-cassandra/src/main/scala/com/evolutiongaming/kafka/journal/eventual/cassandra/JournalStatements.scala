package com.evolutiongaming.kafka.journal.eventual.cassandra

import java.time.Instant

import cats.Monad
import cats.data.{NonEmptyList => Nel}
import cats.implicits._
import com.datastax.driver.core.{BatchStatement, Row, Statement}
import com.evolutiongaming.catshelper.ToTry
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.kafka.journal.eventual.{EventualPayloadAndType, EventualRead, EventualWrite}
import com.evolutiongaming.kafka.journal.eventual.cassandra.CassandraHelper._
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


  trait InsertRecords[F[_]] {
    def apply[A](key: Key, segment: SegmentNr, events: Nel[EventRecord[A]])(implicit W: EventualWrite[F, A]): F[Unit]
  }

  object InsertRecords {

    def of[F[_] : Monad : CassandraSession : ToTry : JsonCodec.Encode](name: TableName): F[InsertRecords[F]] = {

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
           |tags,
           |payload_type,
           |payload_txt,
           |payload_bin,
           |metadata,
           |headers)
           |VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           |""".stripMargin

      for {
        prepared <- query.prepare
      } yield {
        new InsertRecords[F] {
          override def apply[A](key: Key, segment: SegmentNr, events: Nel[EventRecord[A]])(
            implicit W: EventualWrite[F, A]): F[Unit] = {

            def writePayload(payload: A) =
              W.writeEventual(payload)
                .map { payloadAndType =>
                  val (text, bytes) = payloadAndType.payload.fold(
                    text => (text.some, none),
                    bytes => (none, bytes.some)
                  )
                  (payloadAndType.payloadType.some, text, bytes)
                }

            def statementOf(record: EventRecord[A]) = {
              val event = record.event
              event.payload
                .map(writePayload)
                .getOrElse((none[PayloadType], none[String], none[ByteVector]).pure[F])
                .map { case (payloadType, txt, bin) =>
                  prepared
                    .bind()
                    .encode(key)
                    .encode(segment)
                    .encode(event.seqNr)
                    .encode(record.partitionOffset)
                    .encode("timestamp", record.timestamp)
                    .encodeSome(record.origin)
                    .encode("tags", event.tags)
                    .encodeSome("payload_type", payloadType)
                    .encodeSome("payload_txt", txt)
                    .encodeSome("payload_bin", bin)
                    .encode("metadata", record.metadata)(encodeByNameRecordMetadata)
                    .encode(record.headers)
                }
            }

            val statement = {
              if (events.tail.isEmpty) {
                statementOf(events.head).widen[Statement]
              } else {
                events.foldM(new BatchStatement()) { (batch, event) =>
                  statementOf(event).map(batch.add)
                }
                  .widen[Statement]
              }
            }
            statement.flatMap(_.first).void
          }
        }
      }
    }
  }


  trait SelectRecords[F[_]] {

    def apply[A](key: Key, segment: SegmentNr, range: SeqRange)(implicit R: EventualRead[F, A]): Stream[F, EventRecord[A]]
  }

  object SelectRecords {

    def of[F[_] : Monad : CassandraSession : ToTry : JsonCodec.Decode](name: TableName): F[SelectRecords[F]] = {

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

          def apply[A](key: Key, segment: SegmentNr, range: SeqRange)(implicit R: EventualRead[F, A]) = {

            def readPayload(row: Row): F[Option[A]] = {
              val payloadType = row.decode[Option[PayloadType]]("payload_type")
              val payloadTxt = row.decode[Option[String]]("payload_txt")
              val payloadBin = row.decode[Option[ByteVector]]("payload_bin") getOrElse ByteVector.empty

              payloadType
                .map(EventualPayloadAndType(payloadTxt.toLeft(payloadBin), _))
                .traverse(R.readEventual)
            }

            val bound = prepared
              .bind()
              .encode(key)
              .encode(segment)
              .encodeAt(3, range.from)
              .encodeAt(4, range.to)

            for {
              row <- bound.execute
              payload <- Stream.lift(readPayload(row))
            } yield {
              val partitionOffset = row.decode[PartitionOffset]

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
                partitionOffset = partitionOffset,
                metadata = metadata,
                headers = headers)
            }
          }
        }
      }
    }
  }


  trait DeleteRecords[F[_]] {

    def apply(key: Key, segment: SegmentNr, seqNr: SeqNr):  F[Unit]
  }

  object DeleteRecords {

    def of[F[_] : Monad : CassandraSession](name: TableName): F[DeleteRecords[F]] = {
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
          prepared
            .bind()
            .encode(key)
            .encode(segment)
            .encode(seqNr)
            .first
            .void
      }
    }
  }
}

