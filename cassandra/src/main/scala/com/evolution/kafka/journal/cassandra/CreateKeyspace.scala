package com.evolution.kafka.journal.cassandra

import cats.syntax.all.*
import cats.{Applicative, Monad}
import com.evolutiongaming.catshelper.LogOf
import com.evolution.kafka.journal.eventual.cassandra.CassandraHelper.*
import com.evolution.kafka.journal.eventual.cassandra.{CassandraCluster, CassandraSession}
import com.evolutiongaming.scassandra.CreateKeyspaceIfNotExists

private[journal] trait CreateKeyspace[F[_]] {
  def apply(config: KeyspaceConfig): F[Unit]
}

private[journal] object CreateKeyspace {

  def empty[F[_]: Applicative]: CreateKeyspace[F] = (_: KeyspaceConfig) => ().pure[F]

  def apply[F[_]: Monad: CassandraCluster: CassandraSession: LogOf]: CreateKeyspace[F] = new CreateKeyspace[F] {

    def apply(config: KeyspaceConfig): F[Unit] = {
      if (config.autoCreate) {
        val keyspace = config.name

        def create: F[Unit] = {
          val query = CreateKeyspaceIfNotExists(keyspace, config.replicationStrategy)
          for {
            log <- LogOf[F].apply(CreateKeyspace.getClass)
            _ <- log.info(keyspace)
            _ <- query.execute.first
          } yield {}
        }

        for {
          metadata <- CassandraCluster[F].metadata
          metadata <- metadata.keyspace(keyspace)
          result <- metadata.fold(create)(_ => ().pure[F])
        } yield result
      } else {
        ().pure[F]
      }
    }
  }
}
