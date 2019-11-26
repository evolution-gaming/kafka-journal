package com.evolutiongaming.kafka.journal.eventual.cassandra


import java.time.Instant

import cats.Monad
import cats.data.{NonEmptyList => Nel}
import cats.implicits._
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.kafka.journal.eventual.cassandra.CassandraHelper._
import com.evolutiongaming.kafka.journal.util.TemporalHelper._
import com.evolutiongaming.scassandra.TableName
import com.evolutiongaming.scassandra.syntax._

import scala.concurrent.duration.FiniteDuration


// TODO expireAfter: add select by topic,LocalDate
object MetaJournalStatements {

  def createTable(name: TableName): Nel[String] = {

    val table = s"""
      |CREATE TABLE IF NOT EXISTS ${ name.toCql } (
      |id TEXT,
      |topic TEXT,
      |segment BIGINT,
      |partition INT,
      |offset BIGINT,
      |segment_size INT,
      |seq_nr BIGINT,
      |delete_to BIGINT,
      |created TIMESTAMP,
      |created_date DATE,
      |updated TIMESTAMP,
      |expire_on DATE,
      |expire_after DURATION,
      |origin TEXT,
      |properties MAP<TEXT,TEXT>,
      |metadata TEXT,
      |PRIMARY KEY ((topic, segment), id))
      |""".stripMargin

    val createdDateIdx = s"""
      |CREATE INDEX IF NOT EXISTS ${ name.table }_created_date_idx ON ${ name.toCql } (created_date)
      |""".stripMargin

    val expireOnIdx = s"""
      |CREATE INDEX IF NOT EXISTS ${ name.table }_expire_on_idx ON ${ name.toCql } (expire_on)
      |""".stripMargin

    Nel.of(table, createdDateIdx, expireOnIdx)
  }


  trait Insert[F[_]] {

    def apply(
      key: Key,
      segment: SegmentNr,
      created: Instant,
      updated: Instant,
      journalHead: JournalHead,
      origin: Option[Origin]
    ): F[Unit]
  }

  object Insert {

    def of[F[_] : Monad : CassandraSession](name: TableName): F[Insert[F]] = {

      val query =
        s"""
           |INSERT INTO ${ name.toCql } (
           |topic,
           |segment,
           |id,
           |partition,
           |offset,
           |segment_size,
           |seq_nr,
           |delete_to,
           |created,
           |created_date,
           |updated,
           |expire_on,
           |expire_after,
           |origin,
           |properties)
           |VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           |""".stripMargin

      for {
        prepared <- query.prepare
      } yield {
        (key: Key, segment: SegmentNr, created: Instant, updated: Instant, journalHead: JournalHead, origin: Option[Origin]) =>
          prepared
            .bind()
            .encode(key)
            .encode(segment)
            .encode(journalHead)
            .encode("created", created)
            .encode("created_date", created.toLocalDate)
            .encode("updated", updated)
            .encode("expire_on", none[Instant])
            .encode("expire_after", none[FiniteDuration])
            .encodeSome(origin)
            .first
            .void
      }
    }
  }


  trait SelectJournalHead[F[_]] {

    def apply(key: Key, segment: SegmentNr): F[Option[JournalHead]]
  }

  object SelectJournalHead {

    def of[F[_] : Monad : CassandraSession](name: TableName): F[SelectJournalHead[F]] = {
      val query =
        s"""
           |SELECT partition, offset, segment_size, seq_nr, delete_to FROM ${ name.toCql }
           |WHERE id = ?
           |AND topic = ?
           |AND segment = ?
           |""".stripMargin

      for {
        prepared <- query.prepare
      } yield {
        (key: Key, segment: SegmentNr) =>
          val row = prepared
            .bind()
            .encode(key)
            .encode(segment)
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

    def apply(key: Key, segment: SegmentNr): F[Option[JournalPointer]]
  }

  object SelectJournalPointer {

    def of[F[_] : Monad : CassandraSession](name: TableName): F[SelectJournalPointer[F]] = {
      val query =
        s"""
           |SELECT partition, offset, seq_nr FROM ${ name.toCql }
           |WHERE id = ?
           |AND topic = ?
           |AND segment = ?
           |""".stripMargin
      for {
        prepared <- query.prepare
      } yield {
        (key: Key, segment: SegmentNr) =>
          val row = prepared
            .bind()
            .encode(key)
            .encode(segment)
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
      segment: SegmentNr,
      partitionOffset: PartitionOffset,
      timestamp: Instant,
      seqNr: SeqNr,
      deleteTo: SeqNr
    ): F[Unit]
  }

  object Update {

    def of[F[_] : Monad : CassandraSession](name: TableName): F[Update[F]] = {
      val query =
        s"""
           |UPDATE ${ name.toCql }
           |SET partition = ?, offset = ?, seq_nr = ?, delete_to = ?, updated = ?
           |WHERE id = ?
           |AND topic = ?
           |AND segment = ?
           |""".stripMargin

      for {
        prepared <- query.prepare
      } yield {
        (key: Key, segment: SegmentNr, partitionOffset: PartitionOffset, timestamp: Instant, seqNr: SeqNr, deleteTo: SeqNr) =>
          prepared
            .bind()
            .encode(key)
            .encode(segment)
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

    def apply(key: Key, segment: SegmentNr, partitionOffset: PartitionOffset, timestamp: Instant, seqNr: SeqNr): F[Unit]
  }

  object UpdateSeqNr {

    def of[F[_] : Monad : CassandraSession](name: TableName): F[UpdateSeqNr[F]] = {
      val query =
        s"""
           |UPDATE ${ name.toCql }
           |SET partition = ?, offset = ?, seq_nr = ?, updated = ?
           |WHERE id = ?
           |AND topic = ?
           |AND segment = ?
           |""".stripMargin

      for {
        prepared <- query.prepare
      } yield {
        (key: Key, segment: SegmentNr, partitionOffset: PartitionOffset, timestamp: Instant, seqNr: SeqNr) =>
          prepared
            .bind()
            .encode(key)
            .encode(segment)
            .encode(partitionOffset)
            .encode(seqNr)
            .encode("updated", timestamp)
            .first
            .void
      }
    }
  }


  trait UpdateDeleteTo[F[_]] {

    def apply(key: Key, segment: SegmentNr, partitionOffset: PartitionOffset, timestamp: Instant, deleteTo: SeqNr): F[Unit]
  }

  object UpdateDeleteTo {

    def of[F[_] : Monad : CassandraSession](name: TableName): F[UpdateDeleteTo[F]] = {
      val query =
        s"""
           |UPDATE ${ name.toCql }
           |SET partition = ?, offset = ?, delete_to = ?, updated = ?
           |WHERE id = ?
           |AND topic = ?
           |AND segment = ?
           |""".stripMargin

      for {
        prepared <- query.prepare
      } yield {
        (key: Key, segment: SegmentNr, partitionOffset: PartitionOffset, timestamp: Instant, deleteTo: SeqNr) =>
          prepared
            .bind()
            .encode(key)
            .encode(segment)
            .encode(partitionOffset)
            .encode("delete_to", deleteTo)
            .encode("updated", timestamp)
            .first
            .void
      }
    }
  }


  trait Delete[F[_]] {

    def apply(key: Key, segment: SegmentNr): F[Unit]
  }

  object Delete {

    def of[F[_] : Monad : CassandraSession](name: TableName): F[Delete[F]] = {
      val query =
        s"""
           |DELETE FROM ${ name.toCql }
           |WHERE id = ?
           |AND topic = ?
           |AND segment = ?
           |""".stripMargin

      query
        .prepare
        .map { prepared =>
          (key: Key, segment: SegmentNr) =>
            prepared
              .bind()
              .encode(key)
              .encode(segment)
              .first
              .void
        }
    }
  }
}
