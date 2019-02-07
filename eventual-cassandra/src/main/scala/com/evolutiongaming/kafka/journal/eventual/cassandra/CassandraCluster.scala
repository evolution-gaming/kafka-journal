package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.effect.{Concurrent, Resource, Sync}
import cats.implicits._
import com.evolutiongaming.concurrent.CurrentThreadExecutionContext
import com.evolutiongaming.kafka.journal.util.FromFuture
import com.evolutiongaming.scassandra.{CassandraConfig, Cluster, CreateCluster}

trait CassandraCluster[F[_]] {
  def session: Resource[F, CassandraSession[F]]
}

object CassandraCluster {

  def of[F[_] : Concurrent : FromFuture](config: CassandraConfig, retries: Int): Resource[F, CassandraCluster[F]] = {
    for {
      cassandra <- Resource.make {
        Sync[F].delay { CreateCluster(config)(CurrentThreadExecutionContext) }
      } { cassandra =>
        FromFuture[F].apply { cassandra.close() }
      }
    } yield {
      apply[F](cassandra, retries)
    }
  }

  def apply[F[_] : Concurrent : FromFuture](cassandra: Cluster, retries: Int): CassandraCluster[F] = {
    new CassandraCluster[F] {
      def session = {
        val session = for {
          session <- FromFuture[F].apply { cassandra.connect() }
        } yield {
          val session1 = CassandraSession[F](session)
          val session2 = CassandraSession[F](session1, retries)
          val release = FromFuture[F].apply { session.close() }
          (session2, release)
        }
        Resource(session)
      }
    }
  }
}