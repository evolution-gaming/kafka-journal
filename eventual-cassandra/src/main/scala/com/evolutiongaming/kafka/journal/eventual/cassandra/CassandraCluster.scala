package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.effect.{Concurrent, Resource}
import cats.syntax.all._
import com.evolutiongaming.catshelper.FromFuture
import com.evolutiongaming.scassandra.{CassandraClusterOf, CassandraConfig}
import com.evolutiongaming.scassandra
import com.evolutiongaming.scassandra.util.FromGFuture

trait CassandraCluster[F[_]] {

  def session: Resource[F, CassandraSession[F]]

  def metadata: F[CassandraMetadata[F]]
}

object CassandraCluster {

  def apply[F[_]](implicit F: CassandraCluster[F]): CassandraCluster[F] = F

  def apply[F[_] : Concurrent : FromGFuture](
    cluster: scassandra.CassandraCluster[F],
    retries: Int
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

  def of[F[_] : Concurrent : FromFuture : FromGFuture](
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