package com.evolution.kafka.journal.eventual.cassandra

import cats.Parallel
import cats.effect.kernel.Async
import cats.effect.{Concurrent, Resource}
import cats.syntax.all.*
import com.datastax.driver.core.policies.{LoggingRetryPolicy, RetryPolicy}
import com.datastax.driver.core.{ResultSet as _, *}
import com.evolution.scache.Cache
import com.evolutiongaming.catshelper.{MonadThrowable, Runtime}
import com.evolution.kafka.journal.JournalError
import com.evolution.kafka.journal.util.StreamHelper.*
import com.evolutiongaming.scassandra
import com.evolutiongaming.scassandra.NextHostRetryPolicy
import com.evolutiongaming.scassandra.syntax.*
import com.evolutiongaming.scassandra.util.FromGFuture
import com.evolutiongaming.sstream.Stream

trait CassandraSession[F[_]] {

  def prepare(query: String): F[PreparedStatement]

  def execute(statement: Statement): Stream[F, Row]

  def unsafe: scassandra.CassandraSession[F]

  final def execute(statement: String): Stream[F, Row] = execute(new SimpleStatement(statement))
}

object CassandraSession {

  def apply[F[_]](
    implicit
    F: CassandraSession[F],
  ): CassandraSession[F] = F

  def apply[F[_]](
    session: CassandraSession[F],
    retries: Int,
    trace: Boolean = false,
  ): CassandraSession[F] = {
    val retryPolicy = new LoggingRetryPolicy(NextHostRetryPolicy(retries))
    session.configured(retryPolicy, trace)
  }

  private def apply[F[_]: Async: FromGFuture](
    session: scassandra.CassandraSession[F],
  ): CassandraSession[F] = {
    new CassandraSession[F] {

      def prepare(query: String) = session.prepare(query)

      def execute(statement: Statement): Stream[F, Row] = {
        val execute = session.execute(statement)
        for {
          resultSet <- execute.toStream
          row <- ResultSet[F](resultSet)
        } yield row
      }

      def unsafe = session
    }
  }

  def make[F[_]: Async: Parallel: FromGFuture](
    session: scassandra.CassandraSession[F],
  ): Resource[F, CassandraSession[F]] = {
    apply[F](session)
      .enhanceError
      .cachePrepared
  }

  implicit class CassandraSessionOps[F[_]](val self: CassandraSession[F]) extends AnyVal {

    def configured(
      retryPolicy: RetryPolicy,
      trace: Boolean,
    ): CassandraSession[F] = new CassandraSession[F] {

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

    def cachePrepared(implicit
      F: Concurrent[F],
      parallel: Parallel[F],
      runtime: Runtime[F],
    ): Resource[F, CassandraSession[F]] = {
      for {
        cache <- Cache.loading[F, String, PreparedStatement]
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

    def enhanceError(
      implicit
      F: MonadThrowable[F],
    ): CassandraSession[F] = {

      def error[A](msg: String, cause: Throwable) = {
        JournalError(s"CassandraSession.$msg failed with $cause", cause).raiseError[F, A]
      }

      new CassandraSession[F] {

        def prepare(query: String) = {
          self
            .prepare(query)
            .handleErrorWith { a => error(s"prepare query: $query", a) }
        }

        def execute(statement: Statement) = {
          self
            .execute(statement)
            .handleErrorWith { (a: Throwable) => error[Row](s"execute statement: $statement", a).toStream }
        }

        def unsafe = self.unsafe
      }
    }
  }
}
