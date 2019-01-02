package com.evolutiongaming.kafka.journal.util

import cats.effect._
import cats.effect.concurrent.{Deferred, Ref}
import cats.implicits._
import com.evolutiongaming.kafka.journal.util.IOSuite._
import org.scalatest.{AsyncFunSuite, Matchers}

class FiberAndCancelSpec extends AsyncFunSuite with Matchers {

  test("FiberAndCancel") {
    val result = for {
      deferred <- Deferred[IO, Unit]
      ref      <- Ref.of[IO, Boolean](false)
      fiber    <- FiberAndCancel[IO].apply { cancel =>
        Concurrent[IO].start[Unit] {
          for {
            _ <- deferred.complete(())
            _ <- {
              for {
                cancel <- cancel
                _      <- ref.set(cancel)
              } yield {}
            }.foreverM[Unit]
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
