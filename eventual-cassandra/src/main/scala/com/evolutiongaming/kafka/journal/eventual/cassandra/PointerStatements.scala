package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.{Monad, Parallel}
import cats.data.{NonEmptyList => Nel}
import cats.syntax.all._
import com.evolutiongaming.kafka.journal.eventual.cassandra.CassandraHelper._
import com.evolutiongaming.kafka.journal.eventual.cassandra.EventualCassandraConfig.ConsistencyConfig
import com.evolutiongaming.kafka.journal.util.SkafkaHelper._
import com.evolutiongaming.scassandra.TableName
import com.evolutiongaming.scassandra.syntax._
import com.evolutiongaming.skafka.{Offset, Partition, Topic}

import java.time.Instant


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

    def apply[F[_]: Parallel](
      legacy: Insert[F],
      insert: Insert[F],
    ): Insert[F] = {
      (topic: Topic, partition: Partition, offset: Offset, created: Instant, updated: Instant) =>
        legacy(topic, partition, offset, created, updated) &> insert(topic, partition, offset, created, updated)
    }

    def of[F[_]: Monad: CassandraSession](
      name: TableName,
      consistencyConfig: ConsistencyConfig.Write
    ): F[Insert[F]] = {

      val query =
        s"""
           |INSERT INTO ${ name.toCql } (topic, partition, offset, created, updated)
           |VALUES (?, ?, ?, ?, ?)
           |""".stripMargin

      query
        .prepare
        .map { prepared =>
          (topic: Topic, partition: Partition, offset: Offset, created: Instant, updated: Instant) =>
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

    def apply[F[_]: Parallel](
      legacy: Update[F],
      update: Update[F],
    ): Update[F] = {
      (topic: Topic, partition: Partition, offset: Offset, timestamp: Instant) =>
        legacy(topic, partition, offset, timestamp) &> update(topic, partition, offset, timestamp)
    }

    def of[F[_]: Monad: CassandraSession](name: TableName, consistencyConfig: ConsistencyConfig.Write): F[Update[F]] = {

      val query =
        s"""
           |UPDATE ${ name.toCql }
           |SET offset = ?, updated = ?
           |WHERE topic = ?
           |AND partition = ?
           |""".stripMargin

      query
        .prepare
        .map { prepared =>
          (topic: Topic, partition: Partition, offset: Offset, timestamp: Instant) =>
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


  trait Select[F[_]] {

    def apply(topic: Topic, partition: Partition): F[Option[Offset]]
  }

  object Select {

    def apply[F[_]: Monad](
      legacy: Select[F],
      select: Select[F],
    ): Select[F] = {
      (topic: Topic, partition: Partition) =>
        for {
          offset <- select(topic, partition)
          offset <- offset.fold(legacy(topic, partition))(_.some.pure[F])
        } yield offset
    }

    def of[F[_]: Monad: CassandraSession](name: TableName, consistencyConfig: ConsistencyConfig.Read): F[Select[F]] = {

      val query =
        s"""
           |SELECT offset FROM ${ name.toCql }
           |WHERE topic = ?
           |AND partition = ?
           |""".stripMargin

      query
        .prepare
        .map { prepared =>
          (topic: Topic, partition: Partition) =>
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


  trait SelectIn[F[_]] {

    def apply(topic: Topic, partitions: Nel[Partition]): F[Map[Partition, Offset]]
  }

  object SelectIn {

    def apply[F[_]: Monad](
      legacy: SelectIn[F],
      select: SelectIn[F],
    ): SelectIn[F] = {
      (topic: Topic, partitions: Nel[Partition]) =>
        for {
          offsets <- select(topic, partitions)
          offsets <- if (offsets.isEmpty) legacy(topic, partitions) else offsets.pure[F]
        } yield offsets
    }

    def of[F[_]: Monad: CassandraSession](
      name: TableName,
      consistencyConfig: ConsistencyConfig.Read
    ): F[SelectIn[F]] = {

      val query =
        s"""
           |SELECT partition, offset FROM ${ name.toCql }
           |WHERE topic = ?
           |AND partition in :partitions
           |""".stripMargin

      query
        .prepare
        .map { prepared =>
          (topic: Topic, partitions: Nel[Partition]) =>
            prepared
              .bind()
              .encode("topic", topic)
              .encode("partitions", partitions.map(_.value))
              .setConsistencyLevel(consistencyConfig.value)
              .execute
              .toList
              .map { rows =>
                rows
                  .map { row =>
                    val partition = row.decode[Partition]
                    val offset = row.decode[Offset]
                    (partition, offset)
                  }
                  .toMap
              }
        }
    }
  }


  trait SelectTopics[F[_]] {
    def apply(): F[List[Topic]]
  }

  object SelectTopics {

    def apply[F[_]: Monad](
      legacy: SelectTopics[F],
      select: SelectTopics[F],
    ): SelectTopics[F] = {
      () =>
        for {
          topics <- select()
          topics <- if (topics.isEmpty) legacy() else topics.pure[F]
        } yield topics
    }

    def of[F[_]: Monad: CassandraSession](
      name: TableName,
      consistencyConfig: ConsistencyConfig.Read
    ): F[SelectTopics[F]] = {

      val query = s"""SELECT DISTINCT topic FROM ${ name.toCql }""".stripMargin
      query
        .prepare
        .map { prepared =>
          () => {
            prepared
              .bind()
              .setConsistencyLevel(consistencyConfig.value)
              .execute
              .toList
              .map { _.map { _.decode[Topic]("topic") } }
          }
        }
    }
  }

}