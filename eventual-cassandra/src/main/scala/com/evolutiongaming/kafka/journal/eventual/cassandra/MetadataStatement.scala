package com.evolutiongaming.kafka.journal.eventual.cassandra


import com.datastax.driver.core.{Metadata => _}
import com.evolutiongaming.concurrent.CurrentThreadExecutionContext
import com.evolutiongaming.kafka.journal.Alias.{Id, SeqNr}
import com.evolutiongaming.kafka.journal.eventual.cassandra.CassandraHelper._
import com.evolutiongaming.skafka.Topic
import com.evolutiongaming.util.FutureHelper._

import scala.concurrent.Future

object MetadataStatement {

  // TODO Partition metadata table using `topic` column and verify query for all topics works
  def createTable(name: TableName): String = {
    // TODO rename last_seq_nr
    s"""
       |CREATE TABLE IF NOT EXISTS ${ name.asCql } (
       |id text,
       |topic text,
       |segment_size int,
       |delete_to bigint,
       |properties map<text,text>,
       |PRIMARY KEY (id))
       |""".stripMargin
  }


  object Insert {
    type Type = Metadata => Future[Unit]

    def apply(name: TableName, session: PrepareAndExecute): Future[Type] = {
      implicit val ec = CurrentThreadExecutionContext // TODO remove

      val query =
        s"""
           |INSERT INTO ${ name.asCql } (id, topic, segment_size, delete_to, properties)
           |VALUES (?, ?, ?, ?, ?)
           |""".stripMargin

      for {
        prepared <- session.prepare(query)
      } yield {
        metadata: Metadata =>
          val bound = prepared
            .bind()
            .encode("id", metadata.id)
            .encode("topic", metadata.topic)
            .encode("segment_size", metadata.segmentSize)
            .encode("delete_to", metadata.deleteTo)
          val result = session.execute(bound)
          result.unit
      }
    }
  }


  object Select {
    type Type = Id => Future[Option[Metadata]]

    def apply(name: TableName, session: PrepareAndExecute): Future[Type] = {
      implicit val ec = CurrentThreadExecutionContext // TODO remove

      val query =
        s"""
           |SELECT topic, segment_size, delete_to FROM ${ name.asCql }
           |WHERE id = ?
           |""".stripMargin

      for {
        prepared <- session.prepare(query)
      } yield {
        id: Id =>
          val bound = prepared.bind(id)
          for {
            result <- session.execute(bound)
          } yield for {
            row <- Option(result.one()) // TODO use CassandraSession wrapper
          } yield {
            Metadata(
              id = id,
              topic = row.decode[Topic]("topic"),
              segmentSize = row.decode[Int]("segment_size"),
              deleteTo = row.decode[SeqNr]("delete_to"))
          }
      }
    }
  }

  object SelectSegmentSize {
    type Type = Id => Future[Option[Int]]

    def apply(name: TableName, session: PrepareAndExecute): Future[Type] = {
      implicit val ec = CurrentThreadExecutionContext // TODO remove

      val query =
        s"""
           |SELECT segment_size FROM ${ name.asCql }
           |WHERE id = ?
           |""".stripMargin

      for {
        prepared <- session.prepare(query)
      } yield {
        id: Id =>
          val bound = prepared.bind(id)
          for {
            result <- session.execute(bound)
          } yield for {
            row <- Option(result.one())
          } yield {
            row.decode[Int]("segment_size")
          }
      }
    }
  }


  // TODO remove Metadata usage here
  // TODO add separate queries for different cases
  object UpdatedMetadata {
    type Type = Metadata => Future[Unit]

    def apply(name: TableName, session: PrepareAndExecute): Future[Type] = {
      implicit val ec = CurrentThreadExecutionContext // TODO remove

      // TODO use update query
      val query =
        s"""
           |INSERT INTO ${ name.asCql } (id, topic, delete_to)
           |VALUES (?, ?, ?)
           |""".stripMargin

      for {
        prepared <- session.prepare(query)
      } yield {
        metadata: Metadata =>
          val bound = prepared
            .bind()
            .encode("id", metadata.id)
            .encode("topic", metadata.topic)
            .encode("delete_to", metadata.deleteTo)
          val result = session.execute(bound)
          result.unit
      }
    }
  }
}
