package com.evolution.kafka.journal.eventual.cassandra

import cats.Parallel
import cats.effect.{Async, Concurrent, Resource}
import cats.syntax.all.*
import com.datastax.oss.driver.api.core.cql.{PreparedStatement, Row, SimpleStatement, Statement}
import com.evolution.kafka.journal.JournalError
import com.evolution.kafka.journal.util.StreamHelper.*
import com.evolution.scache.Cache
import com.evolution.scassandra
import com.evolution.scassandra.syntax.*
import com.evolutiongaming.catshelper.{MonadThrowable, Runtime}
import com.evolutiongaming.sstream.Stream

trait CassandraSession[F[_]] {

  def prepare(query: String): F[PreparedStatement]

  def execute(statement: Statement[?]): Stream[F, Row]

  def unsafe: scassandra.CassandraSession[F]

  final def execute(statement: String): Stream[F, Row] = execute(SimpleStatement.newInstance(statement))
}

object CassandraSession {

  def apply[F[_]](
    implicit
    F: CassandraSession[F],
  ): CassandraSession[F] = F

  /**
   * Marks all the statements as idempotent, so these are retried by the driver.
   *
   * Unlike the Java driver 3, the retry policy of the Java driver 4 cannot be set per statement, it
   * is configured in `advanced.retry-policy` of an execution profile instead, see
   * [[CassandraCluster.make]].
   */
  def apply[F[_]](
    session: CassandraSession[F],
    trace: Boolean = false,
  ): CassandraSession[F] = {
    session.configured(trace)
  }

  private def apply[F[_]: Async](
    session: scassandra.CassandraSession[F],
  ): CassandraSession[F] = {
    new CassandraSession[F] {

      def prepare(query: String): F[PreparedStatement] = session.prepare(query)

      def execute(statement: Statement[?]): Stream[F, Row] = session.executeStream(statement)

      def unsafe: scassandra.CassandraSession[F] = session
    }
  }

  def make[F[_]: Async: Parallel](
    session: scassandra.CassandraSession[F],
  ): Resource[F, CassandraSession[F]] = {
    apply[F](session)
      .enhanceError
      .cachePrepared
  }

  implicit class CassandraSessionOps[F[_]](val self: CassandraSession[F]) extends AnyVal {

    def configured(
      trace: Boolean,
    ): CassandraSession[F] = new CassandraSession[F] {

      def prepare(query: String): F[PreparedStatement] = {
        self.prepare(query)
      }

      def execute(statement: Statement[?]): Stream[F, Row] = {
        // statements of the Java driver 4 are immutable, the setters return a new instance
        def configure[S <: Statement[S]](statement: Statement[S]): S = {
          statement
            .setIdempotent(true)
            .setTracing(trace)
        }
        self.execute(configure(statement))
      }

      def unsafe: scassandra.CassandraSession[F] = self.unsafe
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

          def prepare(query: String): F[PreparedStatement] = {
            cache.getOrUpdate(query) { self.prepare(query) }
          }

          def execute(statement: Statement[?]): Stream[F, Row] = self.execute(statement)

          def unsafe: scassandra.CassandraSession[F] = self.unsafe
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

        def prepare(query: String): F[PreparedStatement] = {
          self
            .prepare(query)
            .handleErrorWith { a => error(s"prepare query: $query", a) }
        }

        def execute(statement: Statement[?]): Stream[F, Row] = {
          self
            .execute(statement)
            .handleErrorWith { (a: Throwable) => error[Row](s"execute statement: $statement", a).toStream }
        }

        def unsafe: scassandra.CassandraSession[F] = self.unsafe
      }
    }
  }
}
