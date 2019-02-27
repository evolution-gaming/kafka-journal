package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.effect.concurrent.Ref
import cats.effect.{Concurrent, IO}
import cats.implicits._
import com.evolutiongaming.kafka.journal.IOSuite._
import org.scalatest.{AsyncFunSuite, Matchers}

import scala.util.control.NoStackTrace


class ResultSetSpec extends AsyncFunSuite with Matchers {

  for {
    size      <- 0 to 5
    take      <- 1 to 5
    fetchSize <- 1 to 5
  } {
    test(s"size: $size, take: $take, fetchSize: $fetchSize") {
      testF[IO](size = size, take = take, fetchSize = fetchSize).run()
    }
  }

  private def testF[F[_] : Concurrent](size: Int, take: Int, fetchSize: Int) = {

    type Row = Int

    val all = (0 until size).toList

    for {
      left <- Ref[F].of(all.drop(1))
      fetched <- Ref[F].of(all.take(1))
      next = fetched.modify { rows => (List.empty, rows) }
      fetch = for {
        toFetch1 <- left.get
        result <- {
          if (toFetch1.isEmpty) ().pure[F]
          else for {
            taken <- left.modify { rows =>
              val fetched = rows.take(fetchSize)
              val left = rows.drop(fetchSize)
              (left, fetched)
            }
            _ <- fetched.set(taken)
          } yield {}
        }
      } yield result
      resultSet = ResultSet[F, Row](fetch, left.get.map(_.isEmpty), next)
      rows <- resultSet.take(take.toLong).toList
    } yield {
      rows shouldEqual all.take(take)
    }
  }

  case object NotImplemented extends RuntimeException with NoStackTrace
}
