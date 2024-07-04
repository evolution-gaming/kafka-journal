package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.effect.kernel.Temporal
import cats.syntax.all._
import cats.~>
import com.evolutiongaming.cassandra.sync.AutoCreate
import com.evolutiongaming.kafka.journal.Origin
import com.evolutiongaming.kafka.journal.cassandra.{CassandraSync => CassandraSync2}

@deprecated(
  since   = "3.3.9",
  message = "Use a class from `com.evolutiongaming.kafka.journal.cassandra` (without `eventual` part) package instead",
)
trait CassandraSync[F[_]] { self =>

  def apply[A](fa: F[A]): F[A]

  private[cassandra] def toCassandraSync2: CassandraSync2[F] =
    new CassandraSync2[F] {
      def apply[A](fa: F[A]): F[A] = self(fa)
    }

}

@deprecated(
  since   = "3.3.9",
  message = "Use a class from `com.evolutiongaming.kafka.journal.cassandra` (without `eventual` part) package instead",
)
object CassandraSync {

  private[cassandra] def apply[F[_]](cassandraSync2: CassandraSync2[F]): CassandraSync[F] =
    new CassandraSync[F] {
      def apply[A](fa: F[A]): F[A] = cassandraSync2(fa)
    }

  def empty[F[_]]: CassandraSync[F] = {
    val cassandraSync2 = CassandraSync2.empty[F]
    CassandraSync(cassandraSync2)
  }

  def apply[F[_]](implicit F: CassandraSync[F]): CassandraSync[F] = F

  def apply[F[_]: Temporal: CassandraSession](
    config: SchemaConfig,
    origin: Option[Origin],
  ): CassandraSync[F] = {
    val cassandraSync2 = CassandraSync2(config.keyspace.toKeyspaceConfig, config.locksTable, origin)
    CassandraSync(cassandraSync2)
  }

  def apply[F[_]: Temporal: CassandraSession](
    keyspace: String,
    table: String,
    autoCreate: AutoCreate,
    metadata: Option[String],
  ): CassandraSync[F] = {
    val cassandraSync2 = CassandraSync2(keyspace = keyspace, table = table, autoCreate = autoCreate, metadata = metadata)
    CassandraSync(cassandraSync2)
  }

  def of[F[_]: Temporal: CassandraSession](
    config: SchemaConfig,
    origin: Option[Origin],
  ): F[CassandraSync[F]] = {
    val cassandraSync2 = CassandraSync2.of(config.keyspace.toKeyspaceConfig, config.locksTable, origin)
    cassandraSync2.map(CassandraSync(_))
  }

  implicit class CassandraSyncOps[F[_]](val self: CassandraSync[F]) extends AnyVal {

    def mapK[G[_]](fg: F ~> G, gf: G ~> F): CassandraSync[G] =
      CassandraSync(self.toCassandraSync2.mapK(fg, gf))

  }

}
