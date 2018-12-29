package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.effect.IO
import cats.effect.concurrent.Ref
import cats.implicits._
import com.evolutiongaming.kafka.journal.Log
import com.evolutiongaming.kafka.journal.util.IOSuite._
import org.scalatest.{AsyncFunSuite, Matchers}

import scala.concurrent.duration._

class CassandraHealthCheckSpec extends AsyncFunSuite with Matchers {

  test("close") {
    implicit val log = Log.empty[IO]
    val result = for {
      ref         <- Ref.of[IO, Boolean](false)
      healthCheck <- CassandraHealthCheck.of[IO](().pure[IO], 1.second, ref.set(true))
      _           <- healthCheck.close
      stopped     <- 100.tailRecM { n =>
        for {
          stopped <- ref.get
          stopped <- {
            if (stopped || n <= 0) stopped.asRight[Int].pure[IO]
            else for {
              _ <- timer.sleep(5.millis)
            } yield {
              (n - 1).asLeft[Boolean]
            }
          }
        } yield stopped
      }
    } yield {
      stopped shouldEqual true
    }
    result.run
  }
}
