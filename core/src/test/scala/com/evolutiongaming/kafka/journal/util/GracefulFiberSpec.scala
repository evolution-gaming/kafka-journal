package com.evolutiongaming.kafka.journal.util

import cats.effect.{Deferred, Ref, *}
import cats.syntax.all.*
import com.evolutiongaming.kafka.journal.IOSuite.*
import org.scalatest.funsuite.AsyncFunSuite
import org.scalatest.matchers.should.Matchers

class GracefulFiberSpec extends AsyncFunSuite with Matchers {

  test("GracefulFiber") {
    val result = for {
      deferred <- Deferred[IO, Unit]
      ref      <- Ref.of[IO, Boolean](false)
      fiber <- GracefulFiber[IO].apply { cancel =>
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
