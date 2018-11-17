package com.evolutiongaming.kafka.journal.eventual.cassandra

import com.evolutiongaming.scassandra.syntax._
import com.evolutiongaming.concurrent.async.Async
import com.evolutiongaming.kafka.journal.eventual.TopicPointers
import com.evolutiongaming.scassandra.TableName
import com.evolutiongaming.skafka.{Offset, Partition, Topic}

import scala.collection.JavaConverters._

object PointerStatement {

  def createTable(name: TableName): String = {
    s"""
       |CREATE TABLE IF NOT EXISTS ${ name.toCql } (
       |topic text,
       |partition int,
       |offset bigint,
       |created timestamp,
       |updated timestamp,
       |PRIMARY KEY ((topic), partition))
       |""".stripMargin
  }


  object Insert {
    type Type = PointerInsert => Async[Unit]

    def apply(name: TableName, session: PrepareAndExecute): Async[Type] = {
      val query =
        s"""
           |INSERT INTO ${ name.toCql } (topic, partition, offset, created, updated)
           |VALUES (?, ?, ?, ?, ?)
           |""".stripMargin

      for {
        prepared <- session.prepare(query)
      } yield {
        pointer: PointerInsert =>
          val bound = prepared
            .bind()
            .encode("topic", pointer.topic)
            .encode("partition", pointer.partition) // TODO
            .encode("offset", pointer.offset) // TODO
            .encode("created", pointer.created)
            .encode("updated", pointer.updated)
          session.execute(bound).unit
      }
    }
  }

  // TODO not used
  object Update {
    type Type = PointerUpdate => Async[Unit]

    def apply(name: TableName, session: PrepareAndExecute): Async[Type] = {
      val query =
        s"""
           |INSERT INTO ${ name.toCql } (topic, partition, offset, updated)
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


  // TODO not used
  object Select {
    type Type = PointerSelect => Async[Option[Offset]]

    def apply(name: TableName, session: PrepareAndExecute): Async[Type] = {
      val query =
        s"""
           |SELECT offset FROM ${ name.toCql }
           |WHERE topic = ?
           |AND partition = ?
           |""".stripMargin

      for {
        prepared <- session.prepare(query)
      } yield {
        key: PointerSelect =>
          val bound = prepared
            .bind()
            .encode("topic", key.topic)
            .encode("partition", key.partition)
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

  object SelectPointers {
    type Type = Topic => Async[TopicPointers]

    def apply(name: TableName, session: PrepareAndExecute): Async[Type] = {
      val query =
        s"""
           |SELECT partition, offset FROM ${ name.toCql }
           |WHERE topic = ?
           |""".stripMargin

      for {
        prepared <- session.prepare(query)
      } yield {
        topic: Topic =>
          val bound = prepared
            .bind()
            .encode("topic", topic)

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

  object SelectTopics {
    type Type = () => Async[List[Topic]]

    def apply(name: TableName, session: PrepareAndExecute): Async[Type] = {
      val query =
        s"""
           |SELECT DISTINCT topic FROM ${ name.toCql }
           |""".stripMargin
      for {
        prepared <- session.prepare(query)
      } yield {
        () => {
          val bound = prepared.bind()
          for {
            result <- session.execute(bound)
          } yield {
            val rows = result.all().asScala.toList
            for {
              row <- rows
            } yield {
              row.decode[Topic]("topic")
            }
          }
        }
      }
    }
  }
}