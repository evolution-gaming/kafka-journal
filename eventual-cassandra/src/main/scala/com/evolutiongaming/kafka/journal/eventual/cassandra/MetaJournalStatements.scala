package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.Monad
import cats.data.NonEmptyList as Nel
import cats.syntax.all.*
import com.evolutiongaming.kafka.journal.*
import com.evolutiongaming.kafka.journal.cassandra.CassandraConsistencyConfig
import com.evolutiongaming.kafka.journal.cassandra.DeleteToExtension.*
import com.evolutiongaming.kafka.journal.cassandra.KeyExtension.*
import com.evolutiongaming.kafka.journal.cassandra.OriginExtension.*
import com.evolutiongaming.kafka.journal.cassandra.PartitionOffsetExtension.*
import com.evolutiongaming.kafka.journal.cassandra.SeqNrExtension.*
import com.evolutiongaming.kafka.journal.eventual.cassandra.CassandraHelper.*
import com.evolutiongaming.kafka.journal.util.TemporalHelper.*
import com.evolutiongaming.scassandra.TableName
import com.evolutiongaming.scassandra.syntax.*
import com.evolutiongaming.skafka.Topic
import com.evolutiongaming.sstream.Stream

import java.time.{Instant, LocalDate, ZoneOffset}

private[journal] object MetaJournalStatements {

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
      |expire_after DURATION,
      |expire_on DATE,
      |record_id UUID,
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

  def addRecordId(table: TableName): String = {
    s"ALTER TABLE ${ table.toCql } ADD record_id UUID"
  }

  trait Insert[F[_]] {

    def apply(
      key: Key,
      segment: SegmentNr,
      created: Instant,
      updated: Instant,
      journalHead: JournalHead,
      origin: Option[Origin],
    ): F[Unit]
  }

  object Insert {

    def of[F[_]: Monad: CassandraSession](
      name: TableName,
      consistencyConfig: CassandraConsistencyConfig.Write,
    ): F[Insert[F]] = {

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
           |expire_after,
           |expire_on,
           |record_id,
           |origin,
           |properties)
           |VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           |""".stripMargin

      query
        .prepare
        .map {
          prepared => (
            key: Key,
            segment: SegmentNr,
            created: Instant,
            updated: Instant,
            journalHead: JournalHead,
            origin: Option[Origin],
          ) =>
            prepared
              .bind()
              .encode(key)
              .encode(segment)
              .encode(journalHead)
              .encode("created", created)
              .encode("created_date", created.toLocalDate(ZoneOffset.UTC))
              .encode("updated", updated)
              .encodeSome(origin)
              .setConsistencyLevel(consistencyConfig.value)
              .first
              .void
        }
    }
  }

  trait SelectJournalHead[F[_]] {

    def apply(key: Key, segment: SegmentNr): F[Option[JournalHead]]
  }

  object SelectJournalHead {

    def of[F[_]: Monad: CassandraSession](
      name: TableName,
      consistencyConfig: CassandraConsistencyConfig.Read,
    ): F[SelectJournalHead[F]] = {

      val query =
        s"""
           |SELECT partition, offset, segment_size, seq_nr, delete_to, expire_after, expire_on, record_id FROM ${ name.toCql }
           |WHERE id = ?
           |AND topic = ?
           |AND segment = ?
           |""".stripMargin

      query
        .prepare
        .map { prepared => (key: Key, segment: SegmentNr) =>
          val row = prepared
            .bind()
            .encode(key)
            .encode(segment)
            .setConsistencyLevel(consistencyConfig.value)
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

    def of[F[_]: Monad: CassandraSession](
      name: TableName,
      consistencyConfig: CassandraConsistencyConfig.Read,
    ): F[SelectJournalPointer[F]] = {

      val query =
        s"""
           |SELECT partition, offset, seq_nr FROM ${ name.toCql }
           |WHERE id = ?
           |AND topic = ?
           |AND segment = ?
           |""".stripMargin
      query
        .prepare
        .map { prepared => (key: Key, segment: SegmentNr) =>
          val row = prepared
            .bind()
            .encode(key)
            .encode(segment)
            .setConsistencyLevel(consistencyConfig.value)
            .first
          for {
            row <- row
          } yield for {
            row <- row
          } yield {
            JournalPointer(partitionOffset = row.decode[PartitionOffset], seqNr = row.decode[SeqNr])
          }
        }
    }
  }

  trait IdByTopicAndExpireOn[F[_]] {

    def apply(topic: Topic, segment: SegmentNr, expireOn: ExpireOn): Stream[F, String]
  }

  object IdByTopicAndExpireOn {

    def of[F[_]: Monad: CassandraSession](
      name: TableName,
      consistencyConfig: CassandraConsistencyConfig.Read,
    ): F[IdByTopicAndExpireOn[F]] = {

      val query =
        s"""
           |SELECT id FROM ${ name.toCql }
           |WHERE topic = ?
           |AND segment = ?
           |AND expire_on = ?
           |""".stripMargin

      query
        .prepare
        .map { prepared => (topic: Topic, segment: SegmentNr, expireOn: ExpireOn) =>
          prepared
            .bind()
            .encode("topic", topic)
            .encode(segment)
            .encode(expireOn)
            .setConsistencyLevel(consistencyConfig.value)
            .execute
            .map { _.decode[String]("id") }
        }
    }
  }

  trait IdByTopicAndCreated[F[_]] {

    def apply(topic: Topic, segment: SegmentNr, created: LocalDate): Stream[F, String]
  }

  object IdByTopicAndCreated {

    def of[F[_]: Monad: CassandraSession](
      name: TableName,
      consistencyConfig: CassandraConsistencyConfig.Read,
    ): F[IdByTopicAndCreated[F]] = {

      val query =
        s"""
           |SELECT id FROM ${ name.toCql }
           |WHERE topic = ?
           |AND segment = ?
           |AND created_date = ?
           |""".stripMargin

      query
        .prepare
        .map { prepared => (topic: Topic, segment: SegmentNr, created: LocalDate) =>
          prepared
            .bind()
            .encode("topic", topic)
            .encode(segment)
            .encode("created_date", created)
            .setConsistencyLevel(consistencyConfig.value)
            .execute
            .map { _.decode[String]("id") }
        }
    }
  }

  trait IdByTopicAndSegment[F[_]] {

    def apply(topic: Topic, segment: SegmentNr): Stream[F, String]
  }

  object IdByTopicAndSegment {

    def of[F[_]: Monad: CassandraSession](
      name: TableName,
      consistencyConfig: CassandraConsistencyConfig.Read,
    ): F[IdByTopicAndSegment[F]] = {

      val query =
        s"""
           |SELECT id FROM ${ name.toCql }
           |WHERE topic = ?
           |AND segment = ?
           |""".stripMargin

      query
        .prepare
        .map { prepared => (topic: Topic, segment: SegmentNr) =>
          prepared
            .bind()
            .encode("topic", topic)
            .encode(segment)
            .setConsistencyLevel(consistencyConfig.value)
            .execute
            .map { _.decode[String]("id") }
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
      deleteTo: DeleteTo,
    ): F[Unit]
  }

  object Update {

    def of[F[_]: Monad: CassandraSession](
      name: TableName,
      consistencyConfig: CassandraConsistencyConfig.Write,
    ): F[Update[F]] = {

      val query =
        s"""
           |UPDATE ${ name.toCql }
           |SET partition = ?, offset = ?, seq_nr = ?, delete_to = ?, updated = ?
           |WHERE id = ?
           |AND topic = ?
           |AND segment = ?
           |""".stripMargin

      query
        .prepare
        .map {
          prepared => (
            key: Key,
            segment: SegmentNr,
            partitionOffset: PartitionOffset,
            timestamp: Instant,
            seqNr: SeqNr,
            deleteTo: DeleteTo,
          ) =>
            prepared
              .bind()
              .encode(key)
              .encode(segment)
              .encode(partitionOffset)
              .encode(seqNr)
              .encode(deleteTo)
              .encode("updated", timestamp)
              .setConsistencyLevel(consistencyConfig.value)
              .first
              .void
        }
    }
  }

  trait UpdateSeqNr[F[_]] {
    def apply(
      key: Key,
      segment: SegmentNr,
      partitionOffset: PartitionOffset,
      timestamp: Instant,
      seqNr: SeqNr,
    ): F[Unit]
  }

  object UpdateSeqNr {

    def of[F[_]: Monad: CassandraSession](
      name: TableName,
      consistencyConfig: CassandraConsistencyConfig.Write,
    ): F[UpdateSeqNr[F]] = {

      val query =
        s"""
           |UPDATE ${ name.toCql }
           |SET partition = ?, offset = ?, seq_nr = ?, updated = ?
           |WHERE id = ?
           |AND topic = ?
           |AND segment = ?
           |""".stripMargin

      query
        .prepare
        .map {
          prepared => (
            key: Key,
            segment: SegmentNr,
            partitionOffset: PartitionOffset,
            timestamp: Instant,
            seqNr: SeqNr,
          ) =>
            prepared
              .bind()
              .encode(key)
              .encode(segment)
              .encode(partitionOffset)
              .encode(seqNr)
              .encode("updated", timestamp)
              .setConsistencyLevel(consistencyConfig.value)
              .first
              .void
        }
    }
  }

  trait UpdateExpiry[F[_]] {

    def apply(
      key: Key,
      segment: SegmentNr,
      partitionOffset: PartitionOffset,
      timestamp: Instant,
      seqNr: SeqNr,
      expiry: Expiry,
    ): F[Unit]
  }

  object UpdateExpiry {

    def of[F[_]: Monad: CassandraSession](
      name: TableName,
      consistencyConfig: CassandraConsistencyConfig.Write,
    ): F[UpdateExpiry[F]] = {

      val query =
        s"""
           |UPDATE ${ name.toCql }
           |SET partition = ?, offset = ?, seq_nr = ?, updated = ?, expire_after = ?, expire_on = ?
           |WHERE id = ?
           |AND topic = ?
           |AND segment = ?
           |""".stripMargin

      query
        .prepare
        .map {
          prepared => (
            key: Key,
            segment: SegmentNr,
            partitionOffset: PartitionOffset,
            timestamp: Instant,
            seqNr: SeqNr,
            expiry: Expiry,
          ) =>
            prepared
              .bind()
              .encode(key)
              .encode(segment)
              .encode(partitionOffset)
              .encode(seqNr)
              .encode("updated", timestamp)
              .encode(expiry)
              .setConsistencyLevel(consistencyConfig.value)
              .first
              .void
        }
    }
  }

  trait UpdateDeleteTo[F[_]] {

    def apply(
      key: Key,
      segment: SegmentNr,
      partitionOffset: PartitionOffset,
      timestamp: Instant,
      deleteTo: DeleteTo,
    ): F[Unit]
  }

  object UpdateDeleteTo {

    def of[F[_]: Monad: CassandraSession](
      name: TableName,
      consistencyConfig: CassandraConsistencyConfig.Write,
    ): F[UpdateDeleteTo[F]] = {

      val query =
        s"""
           |UPDATE ${ name.toCql }
           |SET partition = ?, offset = ?, delete_to = ?, updated = ?
           |WHERE id = ?
           |AND topic = ?
           |AND segment = ?
           |""".stripMargin

      query
        .prepare
        .map {
          prepared => (
            key: Key,
            segment: SegmentNr,
            partitionOffset: PartitionOffset,
            timestamp: Instant,
            deleteTo: DeleteTo,
          ) =>
            prepared
              .bind()
              .encode(key)
              .encode(segment)
              .encode(partitionOffset)
              .encode(deleteTo)
              .encode("updated", timestamp)
              .setConsistencyLevel(consistencyConfig.value)
              .first
              .void
        }
    }
  }

  trait UpdatePartitionOffset[F[_]] {

    def apply(
      key: Key,
      segment: SegmentNr,
      partitionOffset: PartitionOffset,
      timestamp: Instant,
    ): F[Unit]
  }

  object UpdatePartitionOffset {

    def of[F[_]: Monad: CassandraSession](
      name: TableName,
      consistencyConfig: CassandraConsistencyConfig.Write,
    ): F[UpdatePartitionOffset[F]] = {
      s"""
         |UPDATE ${ name.toCql }
         |SET partition = ?, offset = ?, updated = ?
         |WHERE id = ?
         |AND topic = ?
         |AND segment = ?
         |"""
        .stripMargin
        .prepare
        .map {
          statement => (
            key: Key,
            segment: SegmentNr,
            partitionOffset: PartitionOffset,
            timestamp: Instant,
          ) =>
            statement
              .bind()
              .encode(key)
              .encode(segment)
              .encode(partitionOffset)
              .encode("updated", timestamp)
              .setConsistencyLevel(consistencyConfig.value)
              .first
              .void
        }
    }
  }

  trait Delete[F[_]] {

    def apply(key: Key, segment: SegmentNr): F[Unit]
  }

  object Delete {

    def of[F[_]: Monad: CassandraSession](
      name: TableName,
      consistencyConfig: CassandraConsistencyConfig.Write,
    ): F[Delete[F]] = {

      val query =
        s"""
           |DELETE FROM ${ name.toCql }
           |WHERE id = ?
           |AND topic = ?
           |AND segment = ?
           |""".stripMargin
      query
        .prepare
        .map { prepared => (key: Key, segment: SegmentNr) =>
          prepared
            .bind()
            .encode(key)
            .encode(segment)
            .setConsistencyLevel(consistencyConfig.value)
            .first
            .void
        }
    }
  }

  trait DeleteExpiry[F[_]] {

    def apply(key: Key, segment: SegmentNr): F[Unit]
  }

  object DeleteExpiry {

    def of[F[_]: Monad: CassandraSession](
      name: TableName,
      consistencyConfig: CassandraConsistencyConfig.Write,
    ): F[DeleteExpiry[F]] = {

      val query =
        s"""
           |DELETE expire_after, expire_on FROM ${ name.toCql }
           |WHERE id = ?
           |AND topic = ?
           |AND segment = ?
           |""".stripMargin

      query
        .prepare
        .map { prepared => (key: Key, segmentNr: SegmentNr) =>
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

  trait SelectIds[F[_]] {

    def apply(topic: Topic, segmentNr: SegmentNr): Stream[F, String]
  }

  object SelectIds {

    def of[F[_]: Monad: CassandraSession](
      name: TableName,
      consistencyConfig: CassandraConsistencyConfig.Read,
    ): F[SelectIds[F]] = {
      for {
        prepared <- s"SELECT id FROM ${ name.toCql } WHERE topic = ? AND segment = ?".prepare
      } yield { (topic: Topic, segmentNr: SegmentNr) =>
        prepared
          .bind()
          .encode("topic", topic)
          .encode(segmentNr)
          .setConsistencyLevel(consistencyConfig.value)
          .execute
          .map { _.decode[String]("id") }
      }
    }
  }
}
