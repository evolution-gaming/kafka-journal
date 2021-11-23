package com.evolutiongaming.kafka.journal.eventual.cassandra


import cats.Monad
import cats.syntax.all._
import com.datastax.driver.core.GettableByNameData
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.kafka.journal.eventual.cassandra.CassandraHelper._
import com.evolutiongaming.kafka.journal.eventual.cassandra.EventualCassandraConfig.ConsistencyConfig
import com.evolutiongaming.scassandra.syntax._
import com.evolutiongaming.scassandra.{DecodeRow, EncodeRow, TableName}
import com.evolutiongaming.skafka.Topic
import com.evolutiongaming.sstream.Stream

import java.time.Instant


object MetadataStatements {

  private implicit val encodeRowExpiry: EncodeRow[Option[Expiry]] = EncodeRow.empty

  private implicit val decodeRowExpiry: DecodeRow[Option[Expiry]] = DecodeRow.const(none[Expiry])


  def createTable(name: TableName): String = {
    s"""
       |CREATE TABLE IF NOT EXISTS ${ name.toCql } (
       |id TEXT,
       |topic TEXT,
       |partition INT,
       |offset BIGINT,
       |segment_size INT,
       |seq_nr BIGINT,
       |delete_to BIGINT,
       |created TIMESTAMP,
       |updated TIMESTAMP,
       |origin TEXT,
       |properties MAP<TEXT,TEXT>,
       |metadata TEXT,
       |PRIMARY KEY ((topic), id))
       |""".stripMargin
  }


  trait Insert[F[_]] {

    def apply(key: Key, timestamp: Instant, journalHead: JournalHead, origin: Option[Origin]): F[Unit]
  }

  object Insert {

    def of[F[_]: Monad: CassandraSession](name: TableName)(implicit consistency: ConsistencyConfig.Write): F[Insert[F]] = {

      val query =
        s"""
           |INSERT INTO ${ name.toCql } (id, topic, partition, offset, segment_size, seq_nr, delete_to, created, updated, origin, properties)
           |VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           |""".stripMargin

      for {
        prepared <- query.prepare
      } yield {
        (key: Key, timestamp: Instant, journalHead: JournalHead, origin: Option[Origin]) =>
          prepared
            .bind()
            .encode(key)
            .encode(journalHead)
            .encode("created", timestamp)
            .encode("updated", timestamp)
            .encodeSome(origin)
            .setConsistencyLevel(consistency.value)
            .first
            .void
      }
    }
  }


  trait Select[F[_]] {

    def apply(key: Key): F[Option[MetaJournalEntry]]
  }

  object Select {

    def of[F[_]: Monad: CassandraSession](name: TableName)(implicit consistency: ConsistencyConfig.Read): F[Select[F]] = {
      val query =
        s"""
           |SELECT partition, offset, segment_size, seq_nr, delete_to, created, updated, origin FROM ${ name.toCql }
           |WHERE id = ?
           |AND topic = ?
           |""".stripMargin

      for {
        prepared <- query.prepare
      } yield {
        key: Key =>
          val row = prepared
            .bind()
            .encode(key)
            .setConsistencyLevel(consistency.value)
            .first
          for {
            row <- row
          } yield for {
            row <- row
          } yield {
            row.decode[MetaJournalEntry]
          }
      }
    }
  }


  trait SelectJournalHead[F[_]] {

    def apply(key: Key): F[Option[JournalHead]]
  }

  object SelectJournalHead {

    def of[F[_]: Monad: CassandraSession](name: TableName)(implicit consistency: ConsistencyConfig.Read): F[SelectJournalHead[F]] = {
      val query =
        s"""
           |SELECT partition, offset, segment_size, seq_nr, delete_to FROM ${ name.toCql }
           |WHERE id = ?
           |AND topic = ?
           |""".stripMargin

      for {
        prepared <- query.prepare
      } yield {
        key: Key =>
          val row = prepared
            .bind()
            .encode(key)
            .setConsistencyLevel(consistency.value)
            .first
          for {
            row <- row
          } yield for {
            row <- row
          } yield {
            row.decode[JournalHead]
          }
      }
    }
  }


  trait SelectJournalPointer[F[_]] {

    def apply(key: Key): F[Option[JournalPointer]]
  }

  object SelectJournalPointer {

    def of[F[_]: Monad: CassandraSession](name: TableName)(implicit consistency: ConsistencyConfig.Read): F[SelectJournalPointer[F]] = {
      val query =
        s"""
           |SELECT partition, offset, seq_nr FROM ${ name.toCql }
           |WHERE id = ?
           |AND topic = ?
           |""".stripMargin
      for {
        prepared <- query.prepare
      } yield {
        key: Key =>
          val row = prepared
            .bind()
            .encode(key)
            .setConsistencyLevel(consistency.value)
            .first
          for {
            row <- row
          } yield for {
            row <- row
          } yield {
            JournalPointer(
              partitionOffset = row.decode[PartitionOffset],
              seqNr = row.decode[SeqNr])
          }
      }
    }
  }


  trait Update[F[_]] {

    def apply(
      key: Key,
      partitionOffset: PartitionOffset,
      timestamp: Instant,
      seqNr: SeqNr,
      deleteTo: DeleteTo
    ): F[Unit]
  }

  object Update {

    def of[F[_]: Monad: CassandraSession](name: TableName)(implicit consistency: ConsistencyConfig.Write): F[Update[F]] = {
      val query =
        s"""
           |UPDATE ${ name.toCql }
           |SET partition = ?, offset = ?, seq_nr = ?, delete_to = ?, updated = ?
           |WHERE id = ?
           |AND topic = ?
           |""".stripMargin

      for {
        prepared <- query.prepare
      } yield {
        (key: Key, partitionOffset: PartitionOffset, timestamp: Instant, seqNr: SeqNr, deleteTo: DeleteTo) =>
          prepared
            .bind()
            .encode(key)
            .encode(partitionOffset)
            .encode(seqNr)
            .encode(deleteTo)
            .encode("updated", timestamp)
            .setConsistencyLevel(consistency.value)
            .first
            .void
      }
    }
  }


  trait UpdateSeqNr[F[_]] {

    def apply(key: Key, partitionOffset: PartitionOffset, timestamp: Instant, seqNr: SeqNr): F[Unit]
  }

  object UpdateSeqNr {

    def of[F[_]: Monad: CassandraSession](name: TableName)(implicit consistency: ConsistencyConfig.Write): F[UpdateSeqNr[F]] = {
      val query =
        s"""
           |UPDATE ${ name.toCql }
           |SET partition = ?, offset = ?, seq_nr = ?, updated = ?
           |WHERE id = ?
           |AND topic = ?
           |""".stripMargin

      for {
        prepared <- query.prepare
      } yield {
        (key: Key, partitionOffset: PartitionOffset, timestamp: Instant, seqNr: SeqNr) =>
          prepared
            .bind()
            .encode(key)
            .encode(partitionOffset)
            .encode(seqNr)
            .encode("updated", timestamp)
            .setConsistencyLevel(consistency.value)
            .first
            .void
      }
    }
  }


  trait UpdateDeleteTo[F[_]] {
    
    def apply(key: Key, partitionOffset: PartitionOffset, timestamp: Instant, deleteTo: DeleteTo): F[Unit]
  }

  object UpdateDeleteTo {

    def of[F[_]: Monad: CassandraSession](name: TableName)(implicit consistency: ConsistencyConfig.Write): F[UpdateDeleteTo[F]] = {
      val query =
        s"""
           |UPDATE ${ name.toCql }
           |SET partition = ?, offset = ?, delete_to = ?, updated = ?
           |WHERE id = ?
           |AND topic = ?
           |""".stripMargin

      for {
        prepared <- query.prepare
      } yield {
        (key: Key, partitionOffset: PartitionOffset, timestamp: Instant, deleteTo: DeleteTo) =>
          prepared
            .bind()
            .encode(key)
            .encode(partitionOffset)
            .encode(deleteTo)
            .encode("updated", timestamp)
            .setConsistencyLevel(consistency.value)
            .first
            .void
      }
    }
  }


  trait Delete[F[_]] {

    def apply(key: Key): F[Unit]
  }

  object Delete {

    def of[F[_]: Monad: CassandraSession](name: TableName)(implicit consistency: ConsistencyConfig.Write): F[Delete[F]] = {
      val query =
        s"""
           |DELETE FROM ${ name.toCql }
           |WHERE id = ?
           |AND topic = ?
           |""".stripMargin

      query
        .prepare
        .map { prepared =>
          key: Key =>
            prepared
              .bind()
              .encode(key)
              .setConsistencyLevel(consistency.value)
              .first
              .void
        }
    }
  }


  trait SelectByTopic[F[_]] {

    def apply(topic: Topic): Stream[F, SelectByTopic.Record]
  }

  object SelectByTopic {

    final case class Record(
      id: String,
      journalHead: JournalHead,
      created: Instant,
      updated: Instant,
      origin: Option[Origin])

    object Record {

      implicit val decodeRowEntry: DecodeRow[Record] = {
        row: GettableByNameData => {
          Record(
            id = row.decode[String]("id"),
            journalHead = row.decode[JournalHead],
            created = row.decode[Instant]("created"),
            updated = row.decode[Instant]("updated"),
            origin = row.decode[Option[Origin]])
        }
      }
    }


    def of[F[_]: Monad: CassandraSession](name: TableName, consistency: ConsistencyConfig.Read): F[SelectByTopic[F]] = {
      val query =
        s"""
           |SELECT id, partition, offset, segment_size, seq_nr, delete_to, created, updated, origin FROM ${ name.toCql }
           |WHERE topic = ?
           |""".stripMargin

      for {
        prepared <- query.prepare
      } yield {
        topic: Topic =>
          prepared
            .bind()
            .encode("topic", topic)
            .setConsistencyLevel(consistency.value)
            .execute
            .map { _.decode[Record] }
      }
    }
  }


  trait SelectIds[F[_]] {

    def apply(topic: Topic): Stream[F, String]
  }

  object SelectIds {

    def of[F[_]: Monad: CassandraSession](name: TableName, consistency: ConsistencyConfig.Read): F[SelectIds[F]] = {
      for {
        prepared <- s"SELECT id FROM ${ name.toCql } WHERE topic = ?".prepare
      } yield {
        topic: Topic =>
          prepared
            .bind()
            .encode("topic", topic)
            .setConsistencyLevel(consistency.value)
            .execute
            .map { _.decode[String]("id") }
      }
    }
  }
}
