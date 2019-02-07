package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.FlatMap
import cats.implicits._
import cats.effect.Concurrent
import com.datastax.driver.core._
import com.datastax.driver.core.policies.{LoggingRetryPolicy, RetryPolicy}
import com.evolutiongaming.kafka.journal.util.FromFuture
import com.evolutiongaming.scassandra.{NextHostRetryPolicy, Session}
import com.evolutiongaming.scassandra.syntax._


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
    apply(session, retryPolicy, trace)
  }


  def apply[F[_] : FlatMap](
    session: CassandraSession[F],
    retryPolicy: RetryPolicy,
    trace: Boolean): CassandraSession[F] = new CassandraSession[F] {

    def prepare(query: String) = {
      session.prepare(query)
    }

    def execute(statement: Statement) = {
      val configured = statement
        .setRetryPolicy(retryPolicy)
        .setIdempotent(true)
        .trace(trace)
      session.execute(configured)
    }

    def unsafe = session.unsafe
  }

  def apply[F[_] : Concurrent : FromFuture](session: Session): CassandraSession[F] = {
    new CassandraSession[F] {

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
  }
}