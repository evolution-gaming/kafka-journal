package com.evolutiongaming.kafka.journal.util

import cats.effect._
import cats.effect.concurrent.{Deferred, Ref}
import cats.implicits._
import com.evolutiongaming.kafka.journal.util.IOSuite._
import org.scalatest.{AsyncFunSuite, Matchers}

class GracefulFiberSpec extends AsyncFunSuite with Matchers {

  test("GracefulFiber") {
    val result = for {
      deferred <- Deferred[IO, Unit]
      ref      <- Ref.of[IO, Boolean](false)
      fiber    <- GracefulFiber[IO].apply { cancel =>
        Concurrent[IO].start[Unit] {
          val loop = for {
            cancel <- cancel
            _      <- ref.set(cancel)
          } yield {
            if (cancel) ().some else none
          }
          for {
            _ <- deferred.complete(())
            _ <- loop.untilDefinedM
          } yield {}
        }
      }
      _      <- fiber.cancel
      result <- ref.get
    } yield {
      result shouldEqual true
    }
    result.run()
  }
}
