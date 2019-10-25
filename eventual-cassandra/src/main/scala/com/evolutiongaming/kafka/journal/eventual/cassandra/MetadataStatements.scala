package com.evolutiongaming.kafka.journal.eventual.cassandra


import java.time.Instant

import cats.Monad
import cats.implicits._
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.kafka.journal.eventual.cassandra.CassandraHelper._
import com.evolutiongaming.scassandra.TableName
import com.evolutiongaming.scassandra.syntax._


object MetadataStatements {

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

    def of[F[_]: Monad : CassandraSession](name: TableName): F[Insert[F]] = {

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
            .first
            .void
      }
    }
  }


  trait Select[F[_]] {

    def apply(key: Key): F[Option[MetaJournalEntry]]
  }

  object Select {

    def of[F[_]: Monad : CassandraSession](name: TableName): F[Select[F]] = {
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
            .first
          for {
            row <- row
          } yield for {
            row <- row
          } yield {
            MetaJournalEntry(
              journalHead = row.decode[JournalHead],
              created = row.decode[Instant]("created"),
              updated = row.decode[Instant]("updated"),
              origin = row.decode[Option[Origin]]("origin"))
          }
      }
    }
  }


  trait SelectJournalHead[F[_]] {

    def apply(key: Key): F[Option[JournalHead]]
  }

  object SelectJournalHead {

    def of[F[_]: Monad : CassandraSession](name: TableName): F[SelectJournalHead[F]] = {
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

    def of[F[_]: Monad : CassandraSession](name: TableName): F[SelectJournalPointer[F]] = {
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

    def apply(key: Key, partitionOffset: PartitionOffset, timestamp: Instant, seqNr: SeqNr, deleteTo: SeqNr): F[Unit]
  }

  object Update {

    def of[F[_] : Monad : CassandraSession](name: TableName): F[Update[F]] = {
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
        (key: Key, partitionOffset: PartitionOffset, timestamp: Instant, seqNr: SeqNr, deleteTo: SeqNr) =>
          prepared
            .bind()
            .encode(key)
            .encode(partitionOffset)
            .encode(seqNr)
            .encode("delete_to", deleteTo)
            .encode("updated", timestamp)
            .first
            .void
      }
    }
  }


  trait UpdateSeqNr[F[_]] {
    def apply(key: Key, partitionOffset: PartitionOffset, timestamp: Instant, seqNr: SeqNr): F[Unit]
  }

  object UpdateSeqNr {

    def of[F[_] : Monad : CassandraSession](name: TableName): F[UpdateSeqNr[F]] = {
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
            .first
            .void
      }
    }
  }


  trait UpdateDeleteTo[F[_]] {
    def apply(key: Key, partitionOffset: PartitionOffset, timestamp: Instant, deleteTo: SeqNr): F[Unit]
  }

  object UpdateDeleteTo {

    def of[F[_] : Monad : CassandraSession](name: TableName): F[UpdateDeleteTo[F]] = {
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
        (key: Key, partitionOffset: PartitionOffset, timestamp: Instant, deleteTo: SeqNr) =>
          prepared
            .bind()
            .encode(key)
            .encode(partitionOffset)
            .encode("delete_to", deleteTo)
            .encode("updated", timestamp)
            .first
            .void
      }
    }
  }
}
