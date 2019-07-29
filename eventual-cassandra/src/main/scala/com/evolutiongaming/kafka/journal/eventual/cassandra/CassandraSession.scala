package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.FlatMap
import cats.effect.Concurrent
import cats.implicits._
import com.datastax.driver.core._
import com.datastax.driver.core.policies.{LoggingRetryPolicy, RetryPolicy}
import com.evolutiongaming.kafka.journal.cache.Cache
import com.evolutiongaming.scassandra.syntax._
import com.evolutiongaming.scassandra.NextHostRetryPolicy
import com.evolutiongaming.scassandra
import com.evolutiongaming.scassandra.util.FromGFuture
import com.evolutiongaming.sstream.Stream


trait CassandraSession[F[_]] {

  def prepare(query: String): F[PreparedStatement]

  def execute(statement: Statement): Stream[F, Row]

  def unsafe: scassandra.CassandraSession[F] // TODO remove this

  final def execute(statement: String): Stream[F, Row] = execute(new SimpleStatement(statement))
}


object CassandraSession {

  def apply[F[_]](implicit F: CassandraSession[F]): CassandraSession[F] = F


  def apply[F[_] : FlatMap](
    session: CassandraSession[F],
    retries: Int,
    trace: Boolean = false
  ): CassandraSession[F] = {
    val retryPolicy = new LoggingRetryPolicy(NextHostRetryPolicy(retries))
    session.configured(retryPolicy, trace)
  }


  private def apply[F[_] : Concurrent : FromGFuture](
    session: scassandra.CassandraSession[F]
  ): CassandraSession[F] = {
    new CassandraSession[F] {

      def prepare(query: String) = session.prepare(query)

      def execute(statement: Statement): Stream[F, Row] = {
        val execute = session.execute(statement)
        for {
          resultSet <- Stream.lift(execute)
          row       <- ResultSet[F](resultSet)
        } yield row
      }

      def unsafe = session
    }
  }


  def of[F[_] : Concurrent : FromGFuture](
    session: scassandra.CassandraSession[F]
  ): F[CassandraSession[F]] = {
    apply[F](session).cachePrepared
  }


  implicit class CassandraSessionOps[F[_]](val self: CassandraSession[F]) extends AnyVal {

    def configured(
      retryPolicy: RetryPolicy,
      trace: Boolean): CassandraSession[F] = new CassandraSession[F] {

      def prepare(query: String) = {
        self.prepare(query)
      }

      def execute(statement: Statement) = {
        val configured = statement
          .setRetryPolicy(retryPolicy)
          .setIdempotent(true)
          .trace(trace)
        self.execute(configured)
      }

      def unsafe = self.unsafe
    }


    def cachePrepared(implicit F: Concurrent[F]): F[CassandraSession[F]] = {
      for {
        cache <- Cache.of[F, String, PreparedStatement]
      } yield {
        new CassandraSession[F] {

          def prepare(query: String) = {
            cache.getOrUpdate(query) { self.prepare(query) }
          }

          def execute(statement: Statement) = self.execute(statement)

          def unsafe = self.unsafe
        }
      }
    }
  }
}