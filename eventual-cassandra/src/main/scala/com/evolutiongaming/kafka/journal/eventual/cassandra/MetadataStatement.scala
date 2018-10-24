package com.evolutiongaming.kafka.journal.eventual.cassandra


import java.time.Instant

import com.evolutiongaming.scassandra.CassandraHelper._
import com.evolutiongaming.concurrent.async.Async
import com.evolutiongaming.kafka.journal.{Key, Origin, PartitionOffset, SeqNr}


object MetadataStatement {

  // TODO make use of partition and offset
  def createTable(name: TableName): String = {
    s"""
       |CREATE TABLE IF NOT EXISTS ${ name.asCql } (
       |id text,
       |topic text,
       |partition int,
       |offset bigint,
       |segment_size int,
       |seq_nr bigint,
       |delete_to bigint,
       |created timestamp,
       |updated timestamp,
       |origin text,
       |properties map<text,text>,
       |metadata text,
       |PRIMARY KEY ((topic), id))
       |""".stripMargin
  }


  object Insert {
    type Type = (Key, Instant, Metadata, Option[Origin]) => Async[Unit]

    def apply(name: TableName, session: PrepareAndExecute): Async[Type] = {

      val query =
        s"""
           |INSERT INTO ${ name.asCql } (id, topic, partition, offset, segment_size, seq_nr, delete_to, created, updated, origin, properties)
           |VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           |""".stripMargin

      for {
        prepared <- session.prepare(query)
      } yield {
        (key: Key, timestamp: Instant, metadata: Metadata, origin: Option[Origin]) =>
          val bound = prepared
            .bind()
            .encode(key)
            .encode(metadata.partitionOffset)
            .encode("segment_size", metadata.segmentSize)
            .encode(metadata.seqNr)
            .encode("delete_to", metadata.deleteTo)
            .encode("created", timestamp)
            .encode("updated", timestamp)
            .encode(origin)
          session.execute(bound).unit
      }
    }
  }


  object Select {
    type Type = Key => Async[Option[Metadata]]

    def apply(name: TableName, session: PrepareAndExecute): Async[Type] = {
      val query =
        s"""
           |SELECT partition, offset, segment_size, seq_nr, delete_to FROM ${ name.asCql }
           |WHERE id = ?
           |AND topic = ?
           |""".stripMargin

      for {
        prepared <- session.prepare(query)
      } yield {
        key: Key =>
          val bound = prepared
            .bind()
            .encode(key)
          for {
            result <- session.execute(bound)
          } yield for {
            row <- Option(result.one()) // TODO use CassandraSession wrapper
          } yield {
            Metadata(
              partitionOffset = row.decode[PartitionOffset],
              segmentSize = row.decode[Int]("segment_size"),
              seqNr = row.decode[SeqNr],
              deleteTo = row.decode[Option[SeqNr]]("delete_to"))
          }
      }
    }
  }


  // TODO add classes for common operations
  object Update {
    type Type = (Key, PartitionOffset, Instant, SeqNr, SeqNr) => Async[Unit]

    def apply(name: TableName, session: PrepareAndExecute): Async[Type] = {
      val query =
        s"""
           |UPDATE ${ name.asCql }
           |SET partition = ?, offset = ?, seq_nr = ?, delete_to = ?, updated = ?
           |WHERE id = ?
           |AND topic = ?
           |""".stripMargin

      for {
        prepared <- session.prepare(query)
      } yield {
        (key: Key, partitionOffset: PartitionOffset, timestamp: Instant, seqNr: SeqNr, deleteTo: SeqNr) =>
          val bound = prepared
            .bind()
            .encode(key)
            .encode(partitionOffset)
            .encode(seqNr)
            .encode("delete_to", deleteTo)
            .encode("updated", timestamp)

          session.execute(bound).unit
      }
    }
  }

  // TEST statement
  // TODO add classes for common operations
  object UpdateSeqNr {
    type Type = (Key, PartitionOffset, Instant, SeqNr) => Async[Unit]

    def apply(name: TableName, session: PrepareAndExecute): Async[Type] = {
      val query =
        s"""
           |UPDATE ${ name.asCql }
           |SET partition = ?, offset = ?, seq_nr = ?, updated = ?
           |WHERE id = ?
           |AND topic = ?
           |""".stripMargin

      for {
        prepared <- session.prepare(query)
      } yield {
        (key: Key, partitionOffset: PartitionOffset, timestamp: Instant, seqNr: SeqNr) =>
          val bound = prepared
            .bind()
            .encode(key)
            .encode(partitionOffset)
            .encode(seqNr)
            .encode("updated", timestamp)

          session.execute(bound).unit
      }
    }
  }

  // TODO add classes for common operations
  object UpdateDeleteTo {
    type Type = (Key, PartitionOffset, Instant, SeqNr) => Async[Unit]

    def apply(name: TableName, session: PrepareAndExecute): Async[Type] = {
      val query =
        s"""
           |UPDATE ${ name.asCql }
           |SET partition = ?, offset = ?, delete_to = ?, updated = ?
           |WHERE id = ?
           |AND topic = ?
           |""".stripMargin

      for {
        prepared <- session.prepare(query)
      } yield {
        (key: Key, partitionOffset: PartitionOffset, timestamp: Instant, deleteTo: SeqNr) =>
          val bound = prepared
            .bind()
            .encode(key)
            .encode(partitionOffset)
            .encode("delete_to", deleteTo)
            .encode("updated", timestamp)

          session.execute(bound).unit
      }
    }
  }
}
