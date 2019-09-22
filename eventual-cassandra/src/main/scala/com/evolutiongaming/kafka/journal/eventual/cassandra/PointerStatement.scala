package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.Monad
import cats.implicits._
import com.evolutiongaming.kafka.journal.eventual.TopicPointers
import com.evolutiongaming.kafka.journal.eventual.cassandra.CassandraHelper._
import com.evolutiongaming.scassandra.TableName
import com.evolutiongaming.scassandra.syntax._
import com.evolutiongaming.skafka.{Offset, Partition, Topic, TopicPartition}


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


  trait Insert[F[_]] {
    def apply(pointer: PointerInsert): F[Unit]
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
        pointer: PointerInsert =>
          val bound = prepared
            .bind()
            .encode("topic", pointer.topic)
            .encode("partition", pointer.partition)
            .encode("offset", pointer.offset)
            .encode("created", pointer.created)
            .encode("updated", pointer.updated)
          bound.first.void
      }
    }
  }


  trait Update[F[_]] {
    def apply(pointer: PointerUpdate): F[Unit]
  }

  // TODO not used
  object Update {

    def of[F[_] : Monad : CassandraSession](name: TableName): F[Update[F]] = {
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
          bound.first.void
      }
    }
  }


  trait Select[F[_]] {
    def apply(topicPartition: TopicPartition): F[Option[Offset]]
  }

  // TODO not used
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
        topicPartition: TopicPartition =>
          val bound = prepared
            .bind()
            .encode("topic", topicPartition.topic)
            .encode("partition", topicPartition.partition)
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


  trait SelectPointers[F[_]] {
    def apply(topic: Topic): F[TopicPointers]
  }

  object SelectPointers {

    def of[F[_] : Monad : CassandraSession](name: TableName): F[SelectPointers[F]] = {
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

            TopicPointers(pointers.toMap)
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