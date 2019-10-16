package com.evolutiongaming.kafka.journal.eventual.cassandra

import java.time.Instant

import cats.Monad
import cats.data.{NonEmptyList => Nel}
import cats.implicits._
import com.evolutiongaming.kafka.journal.eventual.cassandra.CassandraHelper._
import com.evolutiongaming.scassandra.TableName
import com.evolutiongaming.scassandra.syntax._
import com.evolutiongaming.skafka.{Offset, Partition, Topic}


object PointerStatements {

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


  trait Insert[F[_]] {
    def apply(topic: Topic, partition: Partition, offset: Offset, created: Instant, updated: Instant): F[Unit]
  }

  object Insert {

    def of[F[_] : Monad : CassandraSession](name: TableName): F[Insert[F]] = {
      val query =
        s"""
           |INSERT INTO ${ name.toCql } (topic, partition, offset, created, updated)
           |VALUES (?, ?, ?, ?, ?)
           |""".stripMargin

      for {
        prepared <- query.prepare
      } yield {
        (topic: Topic, partition: Partition, offset: Offset, created: Instant, updated: Instant) =>
          val bound = prepared
            .bind()
            .encode("topic", topic)
            .encode("partition", partition)
            .encode("offset", offset)
            .encode("created", created)
            .encode("updated", updated)
          bound.first.void
      }
    }
  }


  trait Update[F[_]] {
    def apply(topic: Topic, partition: Partition, offset: Offset, timestamp: Instant): F[Unit]
  }

  object Update {

    def of[F[_] : Monad : CassandraSession](name: TableName): F[Update[F]] = {
      val query =
        s"""
           |UPDATE ${ name.toCql }
           |SET offset = ?, updated = ?
           |WHERE topic = ?
           |AND partition = ?
           |""".stripMargin

      for {
        prepared <- query.prepare
      } yield {
        (topic: Topic, partition: Partition, offset: Offset, timestamp: Instant) =>
          prepared
            .bind()
            .encode("topic", topic)
            .encode("partition", partition)
            .encode("offset", offset)
            .encode("updated", timestamp)
            .first
            .void
      }
    }
  }


  trait Select[F[_]] {
    def apply(topic: Topic, partition: Partition): F[Option[Offset]]
  }

  object Select {

    def of[F[_] : Monad : CassandraSession](name: TableName): F[Select[F]] = {
      val query =
        s"""
           |SELECT offset FROM ${ name.toCql }
           |WHERE topic = ?
           |AND partition = ?
           |""".stripMargin

      for {
        prepared <- query.prepare
      } yield {
        (topic: Topic, partition: Partition) =>
          val bound = prepared
            .bind()
            .encode("topic", topic)
            .encode("partition", partition)
          for {
            row <- bound.first
          } yield for {
            row <- row
          } yield {
            row.decode[Offset]("offset")
          }
      }
    }
  }


  trait SelectIn[F[_]] {
    def apply(topic: Topic, partitions: Nel[Partition]): F[Map[Partition, Offset]]
  }

  object SelectIn {

    def of[F[_] : Monad : CassandraSession](name: TableName): F[SelectIn[F]] = {
      val query =
        s"""
           |SELECT partition, offset FROM ${ name.toCql }
           |WHERE topic = ?
           |AND partition in :partitions
           |""".stripMargin

      for {
        prepared <- query.prepare
      } yield {
        (topic: Topic, partitions: Nel[Partition]) =>

          val bound = prepared
            .bind()
            .encode("topic", topic)
            .encode("partitions", partitions)
          for {
            rows <- bound.execute.toList
          } yield {
            rows
              .map { row =>
                val partition = row.decode[Partition]("partition")
                val offset = row.decode[Offset]("offset")
                (partition, offset)
              }
              .toMap
          }
      }
    }
  }


  trait SelectAll[F[_]] {
    def apply(topic: Topic): F[Map[Partition, Offset]]
  }

  object SelectAll {

    def of[F[_] : Monad : CassandraSession](name: TableName): F[SelectAll[F]] = {
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
            rows <- bound.execute.toList
          } yield {
            val pointers = for {
              row <- rows
            } yield {
              val partition = row.decode[Partition]("partition")
              val offset = row.decode[Offset]("offset")
              (partition, offset)
            }
            pointers.toMap
          }
      }
    }
  }


  trait SelectTopics[F[_]] {
    def apply(): F[List[Topic]]
  }

  object SelectTopics {

    def of[F[_] : Monad : CassandraSession](name: TableName): F[SelectTopics[F]] = {
      val query = s"""SELECT DISTINCT topic FROM ${ name.toCql }""".stripMargin
      for {
        prepared <- query.prepare
      } yield {
        () => {
          val bound = prepared.bind()
          for {
            rows <- bound.execute.toList
          } yield for {
            row <- rows
          } yield {
            row.decode[Topic]("topic")
          }
        }
      }
    }
  }
}