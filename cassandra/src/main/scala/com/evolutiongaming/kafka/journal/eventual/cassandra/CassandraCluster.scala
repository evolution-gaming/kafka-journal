package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.Parallel
import cats.effect.Resource
import cats.effect.kernel.Async
import cats.syntax.all.*
import com.evolutiongaming.scassandra
import com.evolutiongaming.scassandra.util.FromGFuture
import com.evolutiongaming.scassandra.{CassandraClusterOf, CassandraConfig}

trait CassandraCluster[F[_]] {

  def session: Resource[F, CassandraSession[F]]

  def metadata: F[CassandraMetadata[F]]
}

object CassandraCluster {

  def apply[F[_]](implicit F: CassandraCluster[F]): CassandraCluster[F] = F

  def apply[F[_]: Async: Parallel: FromGFuture](
    cluster: scassandra.CassandraCluster[F],
    retries: Int,
  ): CassandraCluster[F] = new CassandraCluster[F] {

    def session = {
      for {
        session <- cluster.connect
        session <- CassandraSession.of[F](session)
      } yield {
        CassandraSession(session, retries)
      }
    }

    def metadata = {
      for {
        metadata <- cluster.metadata
      } yield {
        CassandraMetadata[F](metadata)
      }
    }
  }

  def of[F[_]: Async: Parallel: FromGFuture](
    config: CassandraConfig,
    cassandraClusterOf: CassandraClusterOf[F],
    retries: Int,
  ): Resource[F, CassandraCluster[F]] = {

    for {
      cluster <- cassandraClusterOf(config)
    } yield {
      apply[F](cluster, retries)
    }
  }
}
