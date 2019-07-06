package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.effect.concurrent.Semaphore
import cats.effect.{Concurrent, Sync, Timer}
import cats.implicits._
import com.evolutiongaming.cassandra
import com.evolutiongaming.cassandra.sync.AutoCreate
import com.evolutiongaming.kafka.journal.HostName

trait CassandraSync[F[_]] {
  def apply[A](fa: F[A]): F[A]
}

object CassandraSync {

  def apply[F[_]](implicit F: CassandraSync[F]): CassandraSync[F] = F


  def apply[F[_] : Sync : Timer : CassandraSession](
    config: SchemaConfig,
    semaphore: Semaphore[F]
  ): CassandraSync[F] = {

    val keyspace = config.keyspace
    val autoCreate = if (keyspace.autoCreate) AutoCreate.Table else AutoCreate.None
    apply(
      keyspace = keyspace.name,
      table = config.locksTable,
      autoCreate = autoCreate,
      metadata = HostName().map(_.value),
      semaphore = semaphore)
  }

  def apply[F[_] : Sync : Timer : CassandraSession](
    keyspace: String,
    table: String,
    autoCreate: AutoCreate,
    metadata: Option[String],
    semaphore: Semaphore[F],
  ): CassandraSync[F] = {

    new CassandraSync[F] {

      def apply[A](fa: F[A]) = {

        val cassandraSync = cassandra.sync.CassandraSync.of[F](
          session = CassandraSession[F].unsafe,
          keyspace = keyspace,
          table = table,
          autoCreate = autoCreate)

        for {
          cassandraSync <- cassandraSync
          result        <- semaphore.withPermit {
            cassandraSync(id = "kafka-journal", metadata = metadata)(fa)
          }
        } yield result
      }
    }
  }

  def of[F[_] : Concurrent : Timer : CassandraSession](config: SchemaConfig): F[CassandraSync[F]] = {
    for {
      semaphore <- Semaphore[F](1)
    } yield {
      apply[F](config, semaphore)
    }
  }
}