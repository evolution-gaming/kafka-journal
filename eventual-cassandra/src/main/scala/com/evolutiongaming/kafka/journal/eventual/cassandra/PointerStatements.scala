package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.{Monad, Parallel}
import cats.data.{NonEmptyList => Nel}
import cats.kernel.Semigroup
import cats.syntax.all._
import com.evolutiongaming.catshelper.DataHelper._
import com.evolutiongaming.kafka.journal.eventual.cassandra.CassandraHelper._
import com.evolutiongaming.kafka.journal.eventual.cassandra.EventualCassandraConfig.ConsistencyConfig
import com.evolutiongaming.kafka.journal.util.SkafkaHelper._
import com.evolutiongaming.scassandra.TableName
import com.evolutiongaming.scassandra.syntax._
import com.evolutiongaming.skafka.{Offset, Partition, Topic}

import java.time.Instant
import scala.collection.immutable.SortedSet


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

    implicit class InsertOps[F[_]](val self: Insert[F]) extends AnyVal {

      def andThen(other: Insert[F])(implicit F: Monad[F]): Insert[F] = {
        (topic: Topic, partition: Partition, offset: Offset, created: Instant, updated: Instant) =>
          for {
            a <- self(topic, partition, offset, created, updated)
            b <- other(topic, partition, offset, created, updated)
          } yield {
            a.combine(b)
          }
      }
    }
  }


  trait Update[F[_]] {

    def apply(topic: Topic, partition: Partition, offset: Offset, timestamp: Instant): F[Unit]
  }

  object Update {

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

    implicit class UpdateOps[F[_]](val self: Update[F]) extends AnyVal {

      def both(other: Update[F])(implicit F: Monad[F], P: Parallel[F]): Update[F] = {
        (topic, partition, offset, timestamp) =>
          self(topic, partition, offset, timestamp)
            .parProduct(other(topic, partition, offset, timestamp))
            .map { case (a, b) => a.combine(b) }
      }
    }
  }


  trait Select[F[_]] {

    def apply(topic: Topic, partition: Partition): F[Option[Offset]]
  }

  object Select {

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

    implicit class SelectOps[F[_]](val self: Select[F]) extends AnyVal {

      def orElse(other: Select[F])(implicit F: Monad[F]): Select[F] = {
        (topic: Topic, partition: Partition) => {
          for {
            offset <- self(topic, partition)
            offset <- offset.fold(other(topic, partition))(_.some.pure[F])
          } yield offset
        }
      }
    }
  }


  trait SelectIn[F[_]] {

    def apply(topic: Topic, partitions: Nel[Partition]): F[Map[Partition, Offset]]
  }

  object SelectIn {

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

    implicit class SelectInOps[F[_]](val self: SelectIn[F]) extends AnyVal {

      def both(other: SelectIn[F])(implicit F: Monad[F], P: Parallel[F]): SelectIn[F] = {
        implicit val semigroup: Semigroup[Offset] = _ max _
        (topic, partitions) =>
          self(topic, partitions)
            .parProduct(other(topic, partitions))
            .map { case (a, b) => a.combine(b) }
      }
    }
  }


  trait SelectTopics[F[_]] {
    def apply(): F[SortedSet[Topic]]
  }

  object SelectTopics {

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
              .map { records =>
                records
                  .map { _.decode[Topic]("topic") }
                  .toSortedSet
              }
          }
        }
    }

    implicit class SelectTopicsOps[F[_]](val self: SelectTopics[F]) extends AnyVal {

      def both(other: SelectTopics[F])(implicit F: Monad[F], P: Parallel[F]): SelectTopics[F] = {
        () =>
          self()
            .parProduct(other())
            .map { case (a, b) => a ++ b }
      }
    }
  }

}