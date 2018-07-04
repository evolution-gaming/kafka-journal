package com.evolutiongaming.kafka.journal.ally.cassandra

import java.time.Instant

import com.datastax.driver.core.{Metadata => _}
import com.evolutiongaming.concurrent.CurrentThreadExecutionContext
import com.evolutiongaming.kafka.journal.Alias.{Id, SeqNr}
import com.evolutiongaming.kafka.journal.ally.cassandra.CassandraHelper._
import com.evolutiongaming.skafka.{Offset, Topic}
import com.evolutiongaming.util.FutureHelper._
import java.lang.{Integer => IntJ}

import scala.concurrent.Future

object PointerStatement {

  // TODO Partition metadata table using `topic` column and verify query for all topics works
  def createTable(name: TableName): String = {
    s"""
       |CREATE TABLE IF NOT EXISTS ${ name.asCql } (
       |topic text,
       |partition int,
       |offset bigint,
       |created timestamp,
       |updated timestamp,
       |PRIMARY KEY (topic, partition))
       |""".stripMargin
  }


  object Insert {
    type Type = PointerInsert => Future[Unit]

    def apply(name: TableName, session: PrepareAndExecute): Future[Type] = {
      implicit val ec = CurrentThreadExecutionContext // TODO remove

      val query =
        s"""
           |INSERT INTO ${ name.asCql } (topic, partition, offset, created, updated)
           |VALUES (?, ?, ?, ?, ?)
           |""".stripMargin

      for {
        prepared <- session.prepare(query)
      } yield {
        pointer: PointerInsert =>
          val bound = prepared
            .bind()
            .encode("topic", pointer.topic)
            .encode("partition", pointer.partition)
            .encode("offset", pointer.offset)
            .encode("created", pointer.created)
            .encode("updated", pointer.updated)
          val result = session.execute(bound)
          result.unit
      }
    }
  }

  object Update {
    type Type = PointerUpdate => Future[Unit]

    def apply(name: TableName, session: PrepareAndExecute): Future[Type] = {
      implicit val ec = CurrentThreadExecutionContext // TODO remove

      val query =
        s"""
           |INSERT INTO ${ name.asCql } (topic, partition, offset, created)
           |VALUES (?, ?, ?, ?)
           |""".stripMargin

      for {
        prepared <- session.prepare(query)
      } yield {
        pointer: PointerUpdate =>
          val bound = prepared
            .bind()
            .encode("topic", pointer.topic)
            .encode("partition", pointer.partition)
            .encode("offset", pointer.offset)
            .encode("updated", pointer.updated)
          val result = session.execute(bound)
          result.unit
      }
    }
  }


  object Select {
    type Type = PointerSelect => Future[Option[Offset]]

    def apply(name: TableName, session: PrepareAndExecute): Future[Type] = {
      implicit val ec = CurrentThreadExecutionContext // TODO remove

      val query =
        s"""
           |SELECT offset FROM ${ name.asCql }
           |WHERE topic = ?
           |AND partition = ?
           |""".stripMargin

      for {
        prepared <- session.prepare(query)
      } yield {
        key: PointerSelect =>
          val bound = prepared.bind(key.topic, key.partition: IntJ)
          for {
            result <- session.execute(bound)
          } yield for {
            row <- Option(result.one()) // TODO use CassandraSession wrapper
          } yield {
            row.decode[Offset]("offset")
          }
      }
    }
  }
}