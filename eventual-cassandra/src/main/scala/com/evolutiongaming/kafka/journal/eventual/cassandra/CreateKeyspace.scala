package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.implicits._
import cats.{Applicative, Monad}
import com.evolutiongaming.kafka.journal.LogOf
import com.evolutiongaming.kafka.journal.eventual.cassandra.CassandraHelper._
import com.evolutiongaming.scassandra.CreateKeyspaceIfNotExists

trait CreateKeyspace[F[_]] {
  def apply(config: SchemaConfig.Keyspace): F[Unit]
}

object CreateKeyspace { self =>

  def apply[F[_] : Monad : CassandraCluster : CassandraSession : LogOf]: CreateKeyspace[F] = new CreateKeyspace[F] {
    
    def apply(config: SchemaConfig.Keyspace) = {
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


  def empty[F[_] : Applicative]: CreateKeyspace[F] = new CreateKeyspace[F] {
    def apply(config: SchemaConfig.Keyspace) = ().pure[F]
  }
}
