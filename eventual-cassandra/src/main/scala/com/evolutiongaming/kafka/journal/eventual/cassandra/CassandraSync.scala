package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.arrow.FunctionK
import cats.effect.kernel.Temporal
import cats.effect.std.Semaphore
import cats.syntax.all._
import cats.~>
import com.evolutiongaming.cassandra
import com.evolutiongaming.cassandra.sync.AutoCreate
import com.evolutiongaming.kafka.journal.Origin

trait CassandraSync[F[_]] {
  def apply[A](fa: F[A]): F[A]
}

object CassandraSync {

  def empty[F[_]]: CassandraSync[F] = new CassandraSync[F] {
    def apply[A](fa: F[A]) = fa
  }


  def apply[F[_]](implicit F: CassandraSync[F]): CassandraSync[F] = F


  def apply[F[_] : Temporal : CassandraSession](
    keyspace: KeyspaceConfig,
    locksTable: String,
    origin: Option[Origin],
  ): CassandraSync[F] = {

    val autoCreate = if (keyspace.autoCreate) AutoCreate.Table else AutoCreate.None
    apply(
      keyspace = keyspace.name,
      table = locksTable,
      autoCreate = autoCreate,
      metadata = origin.map(_.value))
  }

  def apply[F[_] : Temporal : CassandraSession](
    keyspace: String,
    table: String,
    autoCreate: AutoCreate,
    metadata: Option[String],
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
          result        <- cassandraSync(id = "kafka-journal", metadata = metadata)(fa)
        } yield result
      }
    }
  }

  def of[F[_] : Temporal : CassandraSession](
    keyspace: KeyspaceConfig,
    locksTable: String,
    origin: Option[Origin]
  ): F[CassandraSync[F]] = {

    for {
      semaphore <- Semaphore[F](1)
    } yield {
      val cassandraSync = apply[F](keyspace, locksTable, origin)
      val serial = new (F ~> F) {
        def apply[A](fa: F[A]) = semaphore.permit.use(_ => fa)
      }
      cassandraSync.mapK(serial, FunctionK.id)
    }
  }


  implicit class CassandraSyncOps[F[_]](val self: CassandraSync[F]) extends AnyVal {

    def mapK[G[_]](fg: F ~> G, gf: G ~> F): CassandraSync[G] = new CassandraSync[G] {

      def apply[A](fa: G[A]) = fg(self(gf(fa)))
    }
  }
}
