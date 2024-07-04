package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.{Applicative, Monad}
import com.evolutiongaming.catshelper.LogOf
import com.evolutiongaming.kafka.journal.cassandra.CreateKeyspace as CreateKeyspace2

@deprecated(
  since   = "3.3.9",
  message = "Use a class from `com.evolutiongaming.kafka.journal.cassandra` (without `eventual` part) package instead",
)
trait CreateKeyspace[F[_]] {
  def apply(config: SchemaConfig.Keyspace): F[Unit]
}

@deprecated(
  since   = "3.3.9",
  message = "Use a class from `com.evolutiongaming.kafka.journal.cassandra` (without `eventual` part) package instead",
)
object CreateKeyspace {

  private[cassandra] def apply[F[_]](createKeyspace2: CreateKeyspace2[F]): CreateKeyspace[F] =
    config => createKeyspace2(config.toKeyspaceConfig)

  def empty[F[_]: Applicative]: CreateKeyspace[F] = {
    val createKeyspace2 = CreateKeyspace2.empty[F]
    CreateKeyspace(createKeyspace2)
  }

  def apply[F[_]: Monad: CassandraCluster: CassandraSession: LogOf]: CreateKeyspace[F] = {
    val createKeyspace2 = CreateKeyspace2[F]
    CreateKeyspace(createKeyspace2)
  }

}
