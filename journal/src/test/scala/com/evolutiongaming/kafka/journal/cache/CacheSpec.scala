package com.evolutiongaming.kafka.journal.cache

import cats.effect.concurrent.Deferred
import cats.effect.{Concurrent, IO}
import cats.implicits._
import com.evolutiongaming.kafka.journal.util.IOSuite._
import org.scalatest.{AsyncFunSuite, Matchers}


class CacheSpec extends AsyncFunSuite with Matchers {

  test("getOrUpdate") {

    val result = for {
      cache     <- Cache.of[IO, Int, Int]
      deferred0 <- Deferred[IO, Unit]
      deferred1 <- Deferred[IO, Unit]
      update0   <- Concurrent[IO].start {
        cache.getOrUpdate(0) {
          for {
            _ <- deferred0.complete(())
            _ <- deferred1.get
          } yield 0
        }
      }
      _         <- deferred0.get
      update1   <- Concurrent[IO].start {
        cache.getOrUpdate(0)(1.pure[IO])
      }
      _ <- deferred1.complete(())
      update0   <- update0.join
      update1   <- update1.join
    } yield {
      update0 shouldEqual 0
      update1 shouldEqual 0
    }
    result.run
  }
}
