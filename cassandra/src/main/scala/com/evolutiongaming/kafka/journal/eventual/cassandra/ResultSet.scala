package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.effect.Sync
import cats.effect.implicits._
import cats.effect.kernel.{Async, Spawn}
import cats.syntax.all._
import com.datastax.driver.core.{ResultSet => ResultSetJ, Row}
import com.evolutiongaming.scassandra.util.FromGFuture
import com.evolutiongaming.sstream.FoldWhile._
import com.evolutiongaming.sstream.Stream

object ResultSet {

  def apply[F[_]: Async: FromGFuture](resultSet: ResultSetJ): Stream[F, Row] = {

    val iterator = resultSet.iterator()

    val fetch = FromGFuture[F].apply { resultSet.fetchMoreResults() }.void

    val fetched = Sync[F].delay { resultSet.isFullyFetched }

    val next = Sync[F].delay { List.fill(resultSet.getAvailableWithoutFetching)(iterator.next()) }

    apply[F, Row](fetch, fetched, next)
  }

  def apply[F[_]: Spawn, A](
    fetch: F[Unit],
    fetched: F[Boolean],
    next: F[List[A]],
  ): Stream[F, A] = new Stream[F, A] {

    def foldWhileM[L, R](l: L)(f: (L, A) => F[Either[L, R]]) = {

      l.tailRecM[F, Either[L, R]] { l =>
        def apply(rows: List[A]) = {
          for {
            result <- rows.foldWhileM(l)(f)
          } yield {
            result.asRight[L]
          }
        }

        def fetchAndApply(rows: List[A]) = {
          for {
            fetching <- fetch.start
            result   <- rows.foldWhileM(l)(f)
            result <- result match {
              case l: Left[L, R]  => fetching.joinWithNever as l.rightCast[Either[L, R]]
              case r: Right[L, R] => r.leftCast[L].asRight[L].pure[F]
            }
          } yield result
        }

        for {
          fetched <- fetched
          rows    <- next
          result  <- if (fetched) apply(rows) else fetchAndApply(rows)
        } yield result
      }
    }
  }
}
