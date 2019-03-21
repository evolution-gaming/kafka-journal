package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.FlatMap
import cats.effect.Concurrent
import cats.implicits._
import com.datastax.driver.core._
import com.datastax.driver.core.policies.{LoggingRetryPolicy, RetryPolicy}
import com.evolutiongaming.catshelper.FromFuture
import com.evolutiongaming.kafka.journal.cache.Cache
import com.evolutiongaming.kafka.journal.stream.Stream
import com.evolutiongaming.kafka.journal.util.FromGFuture
import com.evolutiongaming.scassandra.syntax._
import com.evolutiongaming.scassandra.{NextHostRetryPolicy, Session}


trait CassandraSession[F[_]] {

  def prepare(query: String): F[PreparedStatement]

  def execute(statement: Statement): Stream[F, Row]

  def unsafe: Session // TODO remove this

  final def execute(statement: String): Stream[F, Row] = execute(new SimpleStatement(statement))
}


object CassandraSession {

  def apply[F[_]](implicit F: CassandraSession[F]): CassandraSession[F] = F


  def apply[F[_] : FlatMap](
    session: CassandraSession[F],
    retries: Int,
    trace: Boolean = false): CassandraSession[F] = {

    val retryPolicy = new LoggingRetryPolicy(NextHostRetryPolicy(retries))
    session.configured(retryPolicy, trace)
  }


  private def apply[F[_] : Concurrent : FromFuture : FromGFuture](session: Session): CassandraSession[F] = new CassandraSession[F] {

    def prepare(query: String) = {
      FromFuture[F].apply {
        session.prepare(query)
      }
    }

    def execute(statement: Statement): Stream[F, Row] = {
      val execute = FromFuture[F].apply { session.execute(statement) }
      for {
        resultSet <- Stream.lift(execute)
        row       <- ResultSet[F](resultSet)
      } yield row
    }

    def unsafe = session
  }


  def of[F[_] : Concurrent : FromFuture : FromGFuture](session: Session): F[CassandraSession[F]] = {
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
      } yield new CassandraSession[F] {

        def prepare(query: String) = {
          cache.getOrUpdate(query) { self.prepare(query) }
        }

        def execute(statement: Statement) = self.execute(statement)

        def unsafe = self.unsafe
      }
    }
  }
}