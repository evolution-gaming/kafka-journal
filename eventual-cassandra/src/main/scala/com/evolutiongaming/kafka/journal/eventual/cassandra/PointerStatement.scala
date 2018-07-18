package com.evolutiongaming.kafka.journal.eventual.cassandra

import java.lang.{Integer => IntJ}

import com.datastax.driver.core.{Metadata => _}
import com.evolutiongaming.concurrent.async.Async
import com.evolutiongaming.kafka.journal.eventual.TopicPointers
import com.evolutiongaming.kafka.journal.eventual.cassandra.CassandraHelper._
import com.evolutiongaming.skafka.{Offset, Partition, Topic}

import scala.collection.JavaConverters._

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
    type Type = PointerInsert => Async[Unit]

    def apply(name: TableName, session: PrepareAndExecute): Async[Type] = {
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
          session.execute(bound).unit
      }
    }
  }

  object Update {
    type Type = PointerUpdate => Async[Unit]

    def apply(name: TableName, session: PrepareAndExecute): Async[Type] = {
      val query =
        s"""
           |INSERT INTO ${ name.asCql } (topic, partition, offset, updated)
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
          session.execute(bound).unit
      }
    }
  }


  object Select {
    type Type = PointerSelect => Async[Option[Offset]]

    def apply(name: TableName, session: PrepareAndExecute): Async[Type] = {
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

  object SelectTopicPointers {
    type Type = Topic => Async[TopicPointers]

    def apply(name: TableName, session: PrepareAndExecute): Async[Type] = {
      val query =
        s"""
           |SELECT partition, offset FROM ${ name.asCql }
           |WHERE topic = ?
           |""".stripMargin

      for {
        prepared <- session.prepare(query)
      } yield {
        topic: Topic =>
          val bound = prepared.bind(topic)

          for {
            result <- session.execute(bound)
          } yield {
            val rows = result.all() // TODO blocking

            val pointers = for {
              row <- rows.asScala
            } yield {
              val partition = row.decode[Partition]("partition")
              val offset = row.decode[Offset]("offset")
              (partition, offset)
            }

            TopicPointers(pointers.toMap)
          }
      }
    }
  }
}