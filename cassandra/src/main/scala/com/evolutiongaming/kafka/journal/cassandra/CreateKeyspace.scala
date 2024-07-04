package com.evolutiongaming.kafka.journal.cassandra

import cats.syntax.all._
import cats.{Applicative, Monad}
import com.evolutiongaming.catshelper.LogOf
import com.evolutiongaming.kafka.journal.eventual.cassandra.CassandraHelper._
import com.evolutiongaming.kafka.journal.eventual.cassandra.{CassandraCluster, CassandraSession}
import com.evolutiongaming.scassandra.CreateKeyspaceIfNotExists

trait CreateKeyspace[F[_]] {
  def apply(config: KeyspaceConfig): F[Unit]
}

object CreateKeyspace { self =>

  def empty[F[_]: Applicative]: CreateKeyspace[F] = (_: KeyspaceConfig) => ().pure[F]

  def apply[F[_]: Monad: CassandraCluster: CassandraSession: LogOf]: CreateKeyspace[F] = new CreateKeyspace[F] {

    def apply(config: KeyspaceConfig) = {
      if (config.autoCreate) {
        val keyspace = config.name

        def create = {
          val query = CreateKeyspaceIfNotExists(keyspace, config.replicationStrategy)
          for {
            log <- LogOf[F].apply(self.getClass)
            _   <- log.info(keyspace)
            _   <- query.execute.first
          } yield {}
        }

        for {
          metadata <- CassandraCluster[F].metadata
          metadata <- metadata.keyspace(keyspace)
          result   <- metadata.fold(create)(_ => ().pure[F])
        } yield result
      } else {
        ().pure[F]
      }
    }
  }
}
