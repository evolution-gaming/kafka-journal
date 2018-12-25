package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.Monad
import cats.implicits._
import com.datastax.driver.core.{PreparedStatement, Row, Statement}
import com.evolutiongaming.kafka.journal.FoldWhile._
import com.evolutiongaming.kafka.journal.FoldWhileHelper._

object CassandraHelper {

  implicit class QueryResultOps[F[_]](val self: QueryResult[F]) extends AnyVal {

    def foldWhile[S](s: S)(f: Fold[S, Row])(implicit F: Monad[F]): F[Switch[S]] = {
      
      (s, self).tailRecM { case (s, resultSet) =>
        resultSet.value.fold {
          s.continue.asRight[(S, QueryResult[F])].pure[F]
        } { case (rows, resultSet) =>
          val ss = rows.foldWhile(s)(f)
          if (ss.stop) ss.asRight[(S, QueryResult[F])].pure[F]
          else for {
            resultSet <- resultSet
          } yield (ss.s, resultSet).asLeft
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