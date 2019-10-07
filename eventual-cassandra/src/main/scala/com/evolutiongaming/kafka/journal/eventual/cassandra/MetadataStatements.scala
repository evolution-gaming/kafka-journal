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
    def apply(key: Key, timestamp: Instant, head: Head, origin: Option[Origin]): F[Unit]
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
        (key: Key, timestamp: Instant, head: Head, origin: Option[Origin]) =>
          val bound = prepared
            .bind()
            .encode(key)
            .encode(head.partitionOffset)
            .encode(head.segmentSize)
            .encode(head.seqNr)
            .encodeSome("delete_to", head.deleteTo)
            .encode("created", timestamp)
            .encode("updated", timestamp)
            .encodeSome(origin)
          bound.first.void
      }
    }
  }


  trait Select[F[_]] {
    def apply(key: Key): F[Option[Head]]
  }

  object Select {

    def of[F[_]: Monad : CassandraSession](name: TableName): F[Select[F]] = {
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
          val bound = prepared
            .bind()
            .encode(key)
          for {
            row <- bound.first
          } yield for {
            row <- row
          } yield {
            Head(
              partitionOffset = row.decode[PartitionOffset],
              segmentSize = row.decode[SegmentSize],
              seqNr = row.decode[SeqNr],
              deleteTo = row.decode[Option[SeqNr]]("delete_to"))
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
          val bound = prepared
            .bind()
            .encode(key)
            .encode(partitionOffset)
            .encode(seqNr)
            .encode("delete_to", deleteTo)
            .encode("updated", timestamp)

          bound.first.void
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
          val bound = prepared
            .bind()
            .encode(key)
            .encode(partitionOffset)
            .encode(seqNr)
            .encode("updated", timestamp)
          bound.first.void
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
          val bound = prepared
            .bind()
            .encode(key)
            .encode(partitionOffset)
            .encode("delete_to", deleteTo)
            .encode("updated", timestamp)
          bound.first.void
      }
    }
  }
}
