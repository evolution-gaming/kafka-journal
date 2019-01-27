package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.Monad
import cats.implicits._
import com.datastax.driver.core.{PreparedStatement, Row, Statement}
import com.evolutiongaming.kafka.journal.FoldWhileHelper._
import com.evolutiongaming.kafka.journal.stream.Stream

object CassandraHelper {

  implicit class QueryResultOps[F[_]](val self: QueryResult[F]) extends AnyVal {

    def stream(implicit F: Monad[F]): Stream[F, Row] = new Stream[F, Row] {

      def foldWhileM[L, R](s: L)(f: (L, Row) => F[Either[L, R]]) = {
        (s, self).tailRecM[F, Either[L, R]] { case (s, queryResult) =>
          queryResult.value.fold {
            s.asLeft[R].asRight[(L, QueryResult[F])].pure[F]
          } { case (rows, queryResult) =>
            for {
              result <- rows.foldWhileM(s)(f)
              result <- result match {
                case Left(s) => queryResult.map { queryResult => (s, queryResult).asLeft[Either[L, R]] }
                case result  => result.asRight[(L, QueryResult[F])].pure[F]
              }
            } yield result
          }
        }
      }
    }
  }


  // TODO move to scassandra
  implicit class StatementOps(val self: Statement) extends AnyVal {

    def trace(enable: Boolean): Statement = {
      if (enable) self.enableTracing()
      else self.disableTracing()
    }

    def execute[F[_] : CassandraSession]: F[QueryResult[F]] = {
      CassandraSession[F].execute(self)
    }
  }


  implicit class QueryOps(val self: String) extends AnyVal {

    def prepare[F[_] : CassandraSession]: F[PreparedStatement] = {
      CassandraSession[F].prepare(self)
    }

    def execute[F[_] : CassandraSession]: F[QueryResult[F]] = {
      CassandraSession[F].execute(self)
    }
  }
}