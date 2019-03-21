package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.effect.{Concurrent, Sync}
import cats.effect.concurrent.Semaphore
import cats.implicits._
import com.evolutiongaming.cassandra
import com.evolutiongaming.cassandra.sync.AutoCreate
import com.evolutiongaming.catshelper.{FromFuture, ToFuture}
import com.evolutiongaming.kafka.journal.HostName
import com.evolutiongaming.kafka.journal.util.Executors

trait CassandraSync[F[_]] {
  def apply[A](fa: F[A]): F[A]
}

object CassandraSync {

  def apply[F[_]](implicit F: CassandraSync[F]): CassandraSync[F] = F

  def apply[F[_] : Sync : FromFuture : ToFuture : CassandraSession](
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

  def apply[F[_] : Sync : FromFuture : ToFuture : CassandraSession](
    keyspace: String,
    table: String,
    autoCreate: AutoCreate,
    metadata: Option[String],
    semaphore: Semaphore[F]
  ): CassandraSync[F] = {

    implicit val session = CassandraSession[F].unsafe

    val es = Executors.scheduled(2)

    new CassandraSync[F] {

      def apply[A](fa: F[A]) = {

        es.use { implicit es =>

          val cassandraSync = cassandra.sync.CassandraSync(
            keyspace = keyspace,
            table = table,
            autoCreate = autoCreate)

          semaphore.withPermit {

            FromFuture[F].apply {
              cassandraSync(id = "kafka-journal", metadata = metadata) {
                ToFuture[F].apply(fa)
              }
            }
          }
        }
      }
    }
  }

  def of[F[_] : Concurrent : FromFuture : ToFuture : CassandraSession](config: SchemaConfig): F[CassandraSync[F]] = {
    for {
      semaphore <- Semaphore[F](1)
    } yield {
      apply[F](config, semaphore)
    }
  }
}