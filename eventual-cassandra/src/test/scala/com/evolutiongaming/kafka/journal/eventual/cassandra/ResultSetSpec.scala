package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.effect.{Concurrent, IO}
import cats.syntax.all._
import com.evolutiongaming.kafka.journal.IOSuite._
import org.scalatest.funsuite.AsyncFunSuite
import org.scalatest.matchers.should.Matchers

import scala.util.control.NoStackTrace
import cats.effect.Ref


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
      fetches <- Ref[F].of(0)
      left    <- Ref[F].of(all)
      fetched <- Ref[F].of(List.empty[Row])
      next     = fetched.modify { rows => (List.empty, rows) }
      fetch    = for {
        _        <- fetches.update(_ + 1)
        toFetch1 <- left.get
        result   <- {
          if (toFetch1.isEmpty) ().pure[F]
          else for {
            taken <- left.modify { rows =>
              val fetched = rows.take(fetchSize)
              val left = rows.drop(fetchSize)
              (left, fetched)
            }
            _    <- fetched.set(taken)
          } yield {}
        }
      } yield result
      resultSet   = ResultSet[F, Row](fetch, left.get.map(_.isEmpty), next)
      rows       <- resultSet.take(take.toLong).toList
      fetches    <- fetches.get
    } yield {
      rows shouldEqual all.take(take)

      if (take >= size) {
        val expected = {
          val n = size / fetchSize
          if (size % fetchSize == 0) n else n + 1
        }
        fetches shouldEqual expected
      }
    }
  }

  case object NotImplemented extends RuntimeException with NoStackTrace
}
