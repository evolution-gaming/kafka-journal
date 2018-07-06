package com.evolutiongaming.kafka.journal.eventual.cassandra

import java.time.Instant

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
    s"""
       |CREATE TABLE IF NOT EXISTS ${ name.asCql } (
       |id text,
       |topic text,
       |segment_size int,
       |deleted_to bigint,
       |properties map<text,text>,
       |created timestamp,
       |updated timestamp,
       |PRIMARY KEY (id))
       |""".stripMargin
  }


  object Insert {
    type Type = Metadata => Future[Unit]

    def apply(name: TableName, session: PrepareAndExecute): Future[Type] = {
      implicit val ec = CurrentThreadExecutionContext // TODO remove

      val query =
        s"""
           |INSERT INTO ${ name.asCql } (id, topic, segment_size, deleted_to, properties, created, updated)
           |VALUES (?, ?, ?, ?, ?, ?, ?)
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
            .encode("deleted_to", metadata.deleteTo)
            .encode("created", metadata.created)
            .encode("updated", metadata.updated)
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
           |SELECT topic, segment_size, deleted_to, created, updated FROM ${ name.asCql }
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
              deleteTo = row.decode[SeqNr]("deleted_to"),
              created = row.decode[Instant]("created"),
              updated = row.decode[Instant]("updated"))
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
}
