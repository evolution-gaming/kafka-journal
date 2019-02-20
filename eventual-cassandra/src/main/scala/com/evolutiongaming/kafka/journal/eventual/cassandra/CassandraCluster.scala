package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.effect.{Concurrent, Resource, Sync}
import cats.implicits._
import com.evolutiongaming.kafka.journal.util.FromFuture
import com.evolutiongaming.scassandra.{CassandraConfig, Cluster, CreateCluster}

import scala.concurrent.ExecutionContextExecutor

trait CassandraCluster[F[_]] {

  def session: Resource[F, CassandraSession[F]]

  def metadata: F[CassandraMetadata[F]]
}

object CassandraCluster {

  def apply[F[_]](implicit F: CassandraCluster[F]): CassandraCluster[F] = F

  def apply[F[_] : Concurrent : FromFuture](
    cassandra: Cluster,
    retries: Int
  ): CassandraCluster[F] = new CassandraCluster[F] {

    def session = {
      val session = for {
        session  <- FromFuture[F].apply { cassandra.connect() }
        session1 <- CassandraSession.of[F](session)
      } yield {
        val session2 = CassandraSession[F](session1, retries)
        val release = FromFuture[F].apply { session.close() }
        (session2, release)
      }
      Resource(session)
    }

    def metadata = {
      for {
        metadata <- Sync[F].delay { cassandra.metadata }
      } yield {
        CassandraMetadata[F](metadata)
      }
    }
  }

  def of[F[_] : Concurrent : FromFuture](
    config: CassandraConfig,
    retries: Int,
    executor: ExecutionContextExecutor
  ): Resource[F, CassandraCluster[F]] = {

    for {
      cassandra <- Resource.make {
        Sync[F].delay { CreateCluster(config)(executor) }
      } { cassandra =>
        FromFuture[F].apply { cassandra.close() }
      }
    } yield {
      apply[F](cassandra, retries)
    }
  }
}