package com.evolutiongaming.kafka.journal.eventual.cassandra


import cats.effect.Concurrent
import cats.implicits._
import cats.{FlatMap, Monad}
import com.datastax.driver.core.Row
import com.evolutiongaming.kafka.journal.util.FromFuture
import com.evolutiongaming.nel.Nel


// TODO replace with Stream
final case class QueryResult[F[_] : Monad](value: Option[(Nel[Row], F[QueryResult[F]])]) { self =>

  def head: Option[Row] = value.fold(none[Row]) { case (rows, _) => rows.head.some }

  // TODO refactor
  def all: F[List[Row]] = {
    for {
      rows <- FlatMap[F].tailRecM((self, List.empty[List[Row]])) { case (result, rows) =>
        result.value.fold {
          rows.asRight[(QueryResult[F], List[List[Row]])].pure[F]
        } { case (x, resultSet) =>
          for {
            result <- resultSet
          } yield {
            (result, x.toList :: rows).asLeft
          }
        }
      }
    } yield {
      rows.reverse.flatten
    }
  }
}

object QueryResult {

  def of[F[_] : FlatMap : Concurrent : FromFuture](resultSet: com.datastax.driver.core.ResultSet): F[QueryResult[F]] = {

    val iterator = resultSet.iterator()

    def loop(): F[QueryResult[F]] = {
      val fetched = resultSet.isFullyFetched
      val available = resultSet.getAvailableWithoutFetching
      val rows = List.fill(available)(iterator.next())

      Nel.opt(rows).fold {
        if (fetched) QueryResult[F](none).pure[F] else loop()
      } { rows =>
        for {
          fetch <- Concurrent[F].start {
            for {
              _ <- FromFuture[F].listenable { resultSet.fetchMoreResults() }
              result <- loop()
            } yield {
              result
            }
          }
        } yield {
          QueryResult(Some((rows, fetch.join)))
        }
      }
    }

    loop()
  }
}