package com.evolutiongaming.kafka.journal.cache

import cats.effect.concurrent.Deferred
import cats.effect.{Concurrent, IO}
import cats.implicits._
import com.evolutiongaming.kafka.journal.IOSuite._
import org.scalatest.{AsyncFunSuite, Matchers}

import scala.util.control.NoStackTrace

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
    result.run()
  }

  test("cancellation") {
    val result = for {
      cache     <- Cache.of[IO, Int, Int]
      deferred0 <- Deferred[IO, Unit]
      deferred1 <- Deferred[IO, Int]
      fiber     <- Concurrent[IO].start {
        cache.getOrUpdate(0) {
          for {
            _ <- deferred0.complete(())
            a <- deferred1.get
          } yield a
        }
      }
      _         <- deferred0.get
      _         <- fiber.cancel
      _         <- deferred1.complete(0)
      value     <- cache.get(0)
    } yield {
      value shouldEqual 0.some
    }
    result.run()
  }

  test("no leak in case of failure") {
    val result = for {
      cache     <- Cache.of[IO, Int, Int]
      result0 <- cache.getOrUpdate(0)(TestError.raiseError[IO, Int]).attempt
      result1 <- cache.getOrUpdate(0)(0.pure[IO]).attempt
      result2 <- cache.getOrUpdate(0)(TestError.raiseError[IO, Int]).attempt
    } yield {
      result0 shouldEqual TestError.asLeft[Int]
      result1 shouldEqual 0.asRight
      result2 shouldEqual 0.asRight
    }
    result.run()
  }

  case object TestError extends RuntimeException with NoStackTrace
}
