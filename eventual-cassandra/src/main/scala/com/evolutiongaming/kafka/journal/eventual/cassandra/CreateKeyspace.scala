package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.Monad
import cats.implicits._
import com.evolutiongaming.kafka.journal.LogOf
import com.evolutiongaming.kafka.journal.eventual.cassandra.CassandraHelper._
import com.evolutiongaming.scassandra.CreateKeyspaceIfNotExists

object CreateKeyspace { self =>

  def apply[F[_] : Monad : CassandraCluster : CassandraSession : LogOf](config: SchemaConfig.Keyspace): F[Unit] = {
    if (config.autoCreate) {
      val keyspace = config.name
      def create = {
        val query = CreateKeyspaceIfNotExists(keyspace, config.replicationStrategy)
        for {
          log <- LogOf[F].apply(self.getClass)
          _   <- log.info(keyspace)
          _   <- query.execute
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
