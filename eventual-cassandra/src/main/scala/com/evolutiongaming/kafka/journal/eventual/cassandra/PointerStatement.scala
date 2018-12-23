package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.FlatMap
import cats.implicits._
import com.evolutiongaming.scassandra.syntax._
import com.evolutiongaming.kafka.journal.eventual.TopicPointers
import com.evolutiongaming.kafka.journal.eventual.cassandra.CassandraHelper._
import com.evolutiongaming.kafka.journal.util.CatsHelper._
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
    type Type[F[_]] = PointerInsert => F[Unit]

    def apply[F[_] : FlatMap : CassandraSession](name: TableName): F[Type[F]] = {
      val query =
        s"""
           |INSERT INTO ${ name.toCql } (topic, partition, offset, created, updated)
           |VALUES (?, ?, ?, ?, ?)
           |""".stripMargin

      for {
        prepared <- query.prepare
      } yield {
        pointer: PointerInsert =>
          val bound = prepared
            .bind()
            .encode("topic", pointer.topic)
            .encode("partition", pointer.partition)
            .encode("offset", pointer.offset)
            .encode("created", pointer.created)
            .encode("updated", pointer.updated)
          bound.execute.unit
      }
    }
  }

  // TODO not used
  object Update {
    type Type[F[_]] = PointerUpdate => F[Unit]

    def apply[F[_] : FlatMap : CassandraSession](name: TableName): F[Type[F]] = {
      val query =
        s"""
           |INSERT INTO ${ name.toCql } (topic, partition, offset, updated)
           |VALUES (?, ?, ?, ?)
           |""".stripMargin

      for {
        prepared <- query.prepare
      } yield {
        pointer: PointerUpdate =>
          val bound = prepared
            .bind()
            .encode("topic", pointer.topic)
            .encode("partition", pointer.partition)
            .encode("offset", pointer.offset)
            .encode("updated", pointer.updated)
          bound.execute.unit
      }
    }
  }


  // TODO not used
  object Select {
    type Type[F[_]] = PointerSelect => F[Option[Offset]]

    def apply[F[_] : FlatMap : CassandraSession](name: TableName): F[Type[F]] = {
      val query =
        s"""
           |SELECT offset FROM ${ name.toCql }
           |WHERE topic = ?
           |AND partition = ?
           |""".stripMargin

      for {
        prepared <- query.prepare
      } yield {
        key: PointerSelect =>
          val bound = prepared
            .bind()
            .encode("topic", key.topic)
            .encode("partition", key.partition)
          for {
            result <- bound.execute
          } yield for {
            row <- Option(result.one()) // TODO use CassandraSession wrapper
          } yield {
            row.decode[Offset]("offset")
          }
      }
    }
  }

  object SelectPointers {
    type Type[F[_]] = Topic => F[TopicPointers]

    def apply[F[_] : FlatMap : CassandraSession](name: TableName): F[Type[F]] = {
      val query =
        s"""
           |SELECT partition, offset FROM ${ name.toCql }
           |WHERE topic = ?
           |""".stripMargin

      for {
        prepared <- query.prepare
      } yield {
        topic: Topic =>
          val bound = prepared
            .bind()
            .encode("topic", topic)

          for {
            result <- bound.execute
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
    type Type[F[_]] = () => F[List[Topic]]

    def apply[F[_] : FlatMap : CassandraSession](name: TableName): F[Type[F]] = {
      val query = s"""SELECT DISTINCT topic FROM ${ name.toCql }""".stripMargin
      for {
        prepared <- query.prepare
      } yield {
        () => {
          val bound = prepared.bind()
          for {
            result <- bound.execute
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