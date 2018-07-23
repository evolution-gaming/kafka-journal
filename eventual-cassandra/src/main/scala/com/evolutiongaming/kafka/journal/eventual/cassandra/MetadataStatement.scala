package com.evolutiongaming.kafka.journal.eventual.cassandra


import com.evolutiongaming.concurrent.async.Async
import com.evolutiongaming.kafka.journal.Alias.SeqNr
import com.evolutiongaming.kafka.journal.Key
import com.evolutiongaming.kafka.journal.eventual.cassandra.CassandraHelper._


object MetadataStatement {

  // TODO Partition metadata table using `topic` column and verify query for all topics works
  def createTable(name: TableName): String = {
    s"""
       |CREATE TABLE IF NOT EXISTS ${ name.asCql } (
       |id text,
       |topic text,
       |segment_size int,
       |delete_to bigint,
       |properties map<text,text>,
       |PRIMARY KEY ((topic), id))
       |""".stripMargin
  }


  object Insert {
    type Type = (Key, Metadata) => Async[Unit]

    def apply(name: TableName, session: PrepareAndExecute): Async[Type] = {

      val query =
        s"""
           |INSERT INTO ${ name.asCql } (id, topic, segment_size, delete_to, properties)
           |VALUES (?, ?, ?, ?, ?)
           |""".stripMargin

      for {
        prepared <- session.prepare(query)
      } yield {
        (key: Key, metadata: Metadata) =>
          val bound = prepared
            .bind()
            .encode("id", key.id)
            .encode("topic", key.topic)
            .encode("segment_size", metadata.segmentSize)
            .encode("delete_to", metadata.deleteTo)
          session.execute(bound).unit
      }
    }
  }


  object Select {
    type Type = Key => Async[Option[Metadata]]

    def apply(name: TableName, session: PrepareAndExecute): Async[Type] = {
      val query =
        s"""
           |SELECT segment_size, delete_to FROM ${ name.asCql }
           |WHERE id = ?
           |AND topic = ?
           |""".stripMargin

      for {
        prepared <- session.prepare(query)
      } yield {
        key: Key =>
          val bound = prepared.bind(key.id, key.topic)
          for {
            result <- session.execute(bound)
          } yield for {
            row <- Option(result.one()) // TODO use CassandraSession wrapper
          } yield {
            Metadata(
              segmentSize = row.decode[Int]("segment_size"),
              deleteTo = row.decode[SeqNr]("delete_to"))
          }
      }
    }
  }

  // TODO remove Metadata usage here
  // TODO add separate queries for different cases
  object UpdatedMetadata {
    type Type = (Key, Metadata) => Async[Unit]

    def apply(name: TableName, session: PrepareAndExecute): Async[Type] = {

      // TODO use update query
      val query =
        s"""
           |INSERT INTO ${ name.asCql } (id, topic, delete_to)
           |VALUES (?, ?, ?)
           |""".stripMargin

      for {
        prepared <- session.prepare(query)
      } yield {
        (key: Key, metadata: Metadata) =>
          val bound = prepared
            .bind()
            .encode("id", key.id)
            .encode("topic", key.topic)
            .encode("delete_to", metadata.deleteTo)
          session.execute(bound).unit
      }
    }
  }
}
