package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.effect.{Concurrent, Sync}
import cats.implicits._
import com.datastax.driver.core.{Row, ResultSet => ResultSetJ}
import com.evolutiongaming.kafka.journal.stream.FoldWhile._
import com.evolutiongaming.kafka.journal.stream.Stream
import com.evolutiongaming.kafka.journal.util.FromFuture


object ResultSet {

  def apply[F[_] : Concurrent : FromFuture](resultSet: ResultSetJ): Stream[F, Row] = {

    val iterator = resultSet.iterator()

    val fetch = FromFuture[F].listenable { resultSet.fetchMoreResults() }.void

    val fetched = Sync[F].delay { resultSet.isFullyFetched }

    val next = Sync[F].delay { List.fill(resultSet.getAvailableWithoutFetching)(iterator.next()) }

    apply[F, Row](fetch, fetched, next)
  }

  def apply[F[_] : Concurrent, A](
    fetch: F[Unit],
    fetched: F[Boolean],
    next: F[List[A]]
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
            fetching <- Concurrent[F].start { fetch }
            result   <- rows.foldWhileM(l)(f)
            result   <- result match {
              case Left(l) => fetching.join.as(l.asLeft[Either[L, R]])
              case r       => r.asRight[L].pure[F]
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
