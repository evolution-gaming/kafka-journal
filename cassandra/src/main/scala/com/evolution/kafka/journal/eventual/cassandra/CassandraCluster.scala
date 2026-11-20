package com.evolution.kafka.journal.eventual.cassandra

import cats.Parallel
import cats.effect.{Async, Resource}
import com.evolution.scassandra
import com.evolution.scassandra.{CassandraConfig, NextHostRetryPolicy}
import com.typesafe.config.ConfigValueFactory

/**
 * Provides access to a Cassandra cluster.
 *
 * Unlike the Java driver 3, the session of the Java driver 4 holds the connections and the metadata
 * of the whole cluster, so a single underlying session is shared by all the sessions created by
 * this instance.
 */
trait CassandraCluster[F[_]] {

  def session: Resource[F, CassandraSession[F]]

  def metadata: F[CassandraMetadata[F]]
}

object CassandraCluster {

  def apply[F[_]](
    implicit
    F: CassandraCluster[F],
  ): CassandraCluster[F] = F

  /**
   * Creates a cluster backed by the given session, the session is not closed by the cluster.
   */
  def apply[F[_]: Async: Parallel](
    underlying: scassandra.CassandraSession[F],
  ): CassandraCluster[F] = new CassandraCluster[F] {

    def session: Resource[F, CassandraSession[F]] = {
      CassandraSession.make[F](underlying).map { session => CassandraSession(session) }
    }

    def metadata: F[CassandraMetadata[F]] = {
      Async[F].pure(CassandraMetadata[F](underlying))
    }
  }

  /**
   * Creates a cluster with the given configuration.
   *
   * @param retries
   *   max number of retries of a request, configures [[NextHostRetryPolicy]] as the retry policy of
   *   the default execution profile, overriding `advanced.retry-policy` of `config`
   */
  def make[F[_]: Async: Parallel](
    config: CassandraConfig,
    retries: Int,
  ): Resource[F, CassandraCluster[F]] = {
    for {
      session <- scassandra.CassandraSession.make[F](withRetries(config, retries))
    } yield {
      apply[F](session)
    }
  }

  private[cassandra] def withRetries(config: CassandraConfig, retries: Int): CassandraConfig = {
    val driver = config
      .driver
      .withValue("advanced.retry-policy.class", ConfigValueFactory.fromAnyRef(classOf[NextHostRetryPolicy].getName))
      .withValue(NextHostRetryPolicy.MaxRetries.getPath, ConfigValueFactory.fromAnyRef(retries))
    CassandraConfig(driver)
  }
}
