package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.FlatMap
import cats.effect.Concurrent
import cats.implicits._
import com.datastax.driver.core._
import com.datastax.driver.core.policies.{LoggingRetryPolicy, RetryPolicy}
import com.evolutiongaming.kafka.journal.util.FromFuture
import com.evolutiongaming.scassandra.syntax._
import com.evolutiongaming.scassandra.{NextHostRetryPolicy, Session}


trait CassandraSession[F[_]] {

  def prepare(query: String): F[PreparedStatement]

  def execute(statement: Statement): F[QueryResult[F]]

  def unsafe: Session // TODO remove this

  final def execute(statement: String): F[QueryResult[F]] = execute(new SimpleStatement(statement))
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


  def apply[F[_] : Concurrent : FromFuture](session: Session): CassandraSession[F] = new CassandraSession[F] {

    def prepare(query: String) = {
      FromFuture[F].apply {
        session.prepare(query)
      }
    }

    def execute(statement: Statement) = {
      for {
        result <- FromFuture[F].apply { session.execute(statement) }
        result <- QueryResult.of[F](result)
      } yield {
        result
      }
    }

    def unsafe = session
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
  }
}