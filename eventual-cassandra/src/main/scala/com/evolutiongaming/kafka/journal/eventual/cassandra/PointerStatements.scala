package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.Monad
import cats.syntax.all.*
import com.evolutiongaming.kafka.journal.cassandra.CassandraConsistencyConfig
import com.evolutiongaming.kafka.journal.eventual.cassandra.CassandraHelper.*
import com.evolutiongaming.kafka.journal.util.SkafkaHelper.*
import com.evolutiongaming.scassandra.TableName
import com.evolutiongaming.scassandra.syntax.*
import com.evolutiongaming.skafka.{Offset, Partition, Topic}

import java.time.Instant

object PointerStatements {

  def createTable(name: TableName): String = {
    s"""
       |CREATE TABLE IF NOT EXISTS ${name.toCql} (
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

    def of[F[_]: Monad: CassandraSession](
      name: TableName,
      consistencyConfig: CassandraConsistencyConfig.Write,
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
      consistencyConfig: CassandraConsistencyConfig.Write,
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
}
