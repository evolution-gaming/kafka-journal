package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.Monad
import cats.syntax.all.*
import com.datastax.driver.core.GettableByNameData
import com.evolutiongaming.catshelper.DataHelper.*
import com.evolutiongaming.kafka.journal.eventual.cassandra.CassandraHelper.*
import com.evolutiongaming.kafka.journal.util.SkafkaHelper.*
import com.evolutiongaming.scassandra.syntax.*
import com.evolutiongaming.scassandra.{DecodeRow, TableName}
import com.evolutiongaming.skafka.{Offset, Partition, Topic}

import java.time.Instant
import scala.collection.immutable.SortedSet

object Pointer2Statements {

  def createTable(name: TableName): String = {
    s"""
       |CREATE TABLE IF NOT EXISTS ${name.toCql} (
       |topic text,
       |partition int,
       |offset bigint,
       |created timestamp,
       |updated timestamp,
       |PRIMARY KEY ((topic, partition)))
       |""".stripMargin
  }

  trait SelectTopics[F[_]] {
    def apply(): F[SortedSet[Topic]]
  }

  object SelectTopics {

    def of[F[_]: Monad: CassandraSession](
      name: TableName,
      consistencyConfig: EventualCassandraConfig.ConsistencyConfig.Read,
    ): F[SelectTopics[F]] = {

      val query = s"""SELECT DISTINCT topic, partition FROM ${name.toCql}""".stripMargin

      query
        .prepare
        .map { prepared => () =>
          {
            prepared
              .bind()
              .setConsistencyLevel(consistencyConfig.value)
              .execute
              .toList
              .map { records =>
                records.map { _.decode[Topic]("topic") }.toSortedSet
              }
          }
        }
    }
  }

  trait Select[F[_]] {

    def apply(topic: Topic, partition: Partition): F[Option[Select.Result]]
  }

  object Select {

    final case class Result(created: Option[Instant])

    object Result {
      implicit val decodeResult: DecodeRow[Result] = { (row: GettableByNameData) =>
        {
          Result(row.decode[Option[Instant]]("created"))
        }
      }
    }

    def of[F[_]: Monad: CassandraSession](
      name: TableName,
      consistencyConfig: EventualCassandraConfig.ConsistencyConfig.Read,
    ): F[Select[F]] = {
      s"""
         |SELECT created FROM ${name.toCql}
         |WHERE topic = ?
         |AND partition = ?
         |"""
        .stripMargin
        .prepare
        .map { prepared => (topic: Topic, partition: Partition) =>
          prepared
            .bind()
            .encode("topic", topic)
            .encode("partition", partition)
            .setConsistencyLevel(consistencyConfig.value)
            .first
            .map { _.map { _.decode[Result] } }
        }
    }
  }

  trait SelectOffset[F[_]] {

    def apply(topic: Topic, partition: Partition): F[Option[Offset]]
  }

  object SelectOffset {

    def of[F[_]: Monad: CassandraSession](
      name: TableName,
      consistencyConfig: EventualCassandraConfig.ConsistencyConfig.Read,
    ): F[SelectOffset[F]] = {

      val query =
        s"""
           |SELECT offset FROM ${name.toCql}
           |WHERE topic = ?
           |AND partition = ?
           |""".stripMargin

      query
        .prepare
        .map { prepared => (topic: Topic, partition: Partition) =>
          prepared
            .bind()
            .encode("topic", topic)
            .encode("partition", partition)
            .setConsistencyLevel(consistencyConfig.value)
            .first
            .map { _.map { _.decode[Offset]("offset") } }
        }
    }
  }

  trait Insert[F[_]] {

    def apply(topic: Topic, partition: Partition, offset: Offset, created: Instant, updated: Instant): F[Unit]
  }

  object Insert {

    def of[F[_]: Monad: CassandraSession](
      name: TableName,
      consistencyConfig: EventualCassandraConfig.ConsistencyConfig.Write,
    ): F[Insert[F]] = {

      val query =
        s"""
           |INSERT INTO ${name.toCql} (topic, partition, offset, created, updated)
           |VALUES (?, ?, ?, ?, ?)
           |""".stripMargin

      query
        .prepare
        .map { prepared => (topic: Topic, partition: Partition, offset: Offset, created: Instant, updated: Instant) =>
          prepared
            .bind()
            .encode("topic", topic)
            .encode(partition)
            .encode(offset)
            .encode("created", created)
            .encode("updated", updated)
            .setConsistencyLevel(consistencyConfig.value)
            .first
            .void
        }
    }
  }

  trait Update[F[_]] {

    def apply(topic: Topic, partition: Partition, offset: Offset, timestamp: Instant): F[Unit]
  }

  object Update {

    def of[F[_]: Monad: CassandraSession](
      name: TableName,
      consistencyConfig: EventualCassandraConfig.ConsistencyConfig.Write,
    ): F[Update[F]] = {

      val query =
        s"""
           |UPDATE ${name.toCql}
           |SET offset = ?, updated = ?
           |WHERE topic = ?
           |AND partition = ?
           |""".stripMargin

      query
        .prepare
        .map { prepared => (topic: Topic, partition: Partition, offset: Offset, timestamp: Instant) =>
          prepared
            .bind()
            .encode("topic", topic)
            .encode("partition", partition)
            .encode("offset", offset)
            .encode("updated", timestamp)
            .setConsistencyLevel(consistencyConfig.value)
            .first
            .void
        }
    }
  }

  // TODO MR remove with next major release
  trait UpdateCreated[F[_]] {

    def apply(topic: Topic, partition: Partition, offset: Offset, created: Instant, updated: Instant): F[Unit]
  }

  object UpdateCreated {

    def of[F[_]: Monad: CassandraSession](
      name: TableName,
      consistencyConfig: EventualCassandraConfig.ConsistencyConfig.Write,
    ): F[UpdateCreated[F]] = {
      s"""
         |UPDATE ${name.toCql}
         |SET offset = ?, created = ?, updated = ?
         |WHERE topic = ?
         |AND partition = ?
         |"""
        .stripMargin
        .prepare
        .map { prepared => (topic: Topic, partition: Partition, offset: Offset, created: Instant, updated: Instant) =>
          prepared
            .bind()
            .encode("topic", topic)
            .encode("partition", partition)
            .encode("offset", offset)
            .encode("created", created)
            .encode("updated", updated)
            .setConsistencyLevel(consistencyConfig.value)
            .first
            .void
        }
    }
  }
}
