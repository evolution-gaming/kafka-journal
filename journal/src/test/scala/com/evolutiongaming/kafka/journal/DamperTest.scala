package com.evolutiongaming.kafka.journal

import cats.effect.{IO, Ref}
import cats.effect.unsafe.implicits.global
import cats.syntax.all._
import org.scalatest.funsuite.AsyncFunSuite
import org.scalatest.matchers.should.Matchers
import com.evolutiongaming.kafka.journal.IOSuite._

import scala.concurrent.TimeoutException
import scala.concurrent.duration._

class DamperTest extends AsyncFunSuite with Matchers {

  test(".release unblocks .acquire") {
    val result = for {
      damper <- Damper.of[IO](a => (a % 2) * 500.millis)
      _      <- damper.acquire
      fiber  <- damper.acquire.start
      result <- fiber.joinWithNever.timeout(10.millis).attempt
      _      <- IO { result should matchPattern { case Left(_: TimeoutException) => () } }
      _      <- damper.release
      _      <- fiber.joinWithNever
    } yield {}
    result.run()
  }

  test(".release unblocks multiple .acquire") {
    val result = for {
      damper <- Damper.of[IO](a => (a % 2) * 500.millis)
      _      <- damper.acquire
      fiber0 <- damper.acquire.start
      result <- fiber0.joinWithNever.timeout(10.millis).attempt
      _      <- IO { result should matchPattern { case Left(_: TimeoutException) => () } }
      fiber1 <- damper.acquire.start
      result <- fiber1.joinWithNever.timeout(10.millis).attempt
      _      <- IO { result should matchPattern { case Left(_: TimeoutException) => () } }
      _      <- damper.release
      _      <- fiber0.joinWithNever
      _      <- fiber1.joinWithNever
    } yield {}
    result.run()
  }

  test("cancel") {
    val result = for {
      ref    <- Ref[IO].of(List(1.minute, 0.minutes))
      damper <- Damper.of[IO] { _ =>
        ref
          .modify {
            case Nil     => (Nil, 1.minute)
            case a :: as => (as, a)
          }
          .unsafeRunSync()
      }

      fiber0 <- damper.acquire.start
      result <- fiber0.joinWithNever.timeout(10.millis).attempt
      _      <- IO { result should matchPattern { case Left(_: TimeoutException) => () } }

      fiber1 <- damper.acquire.start
      result <- fiber1.joinWithNever.timeout(10.millis).attempt
      _      <- IO { result should matchPattern { case Left(_: TimeoutException) => () } }

      fiber2 <- damper.acquire.start
      result <- fiber2.joinWithNever.timeout(10.millis).attempt
      _      <- IO { result should matchPattern { case Left(_: TimeoutException) => () } }

      _      <- fiber1.cancel
      _      <- fiber0.cancel
      _      <- fiber2.joinWithNever
      _      <- damper.release
    } yield {}
    result.run()
  }

  test("many") {
    val result = for {
      last    <- Ref[IO].of(0)
      max     <- Ref[IO].of(0)
      damper  <- Damper.of[IO] { acquired =>
        val result = for {
          _ <- last.set(acquired)
          _ <- max.update { _ max acquired }
        } yield {
          1.millis
        }
        result.unsafeRunSync()
      }
      _      <- damper
        .resource
        .use { _ => IO.sleep(100.millis) }
        .timeoutTo(1000.millis, IO.unit)
        .parReplicateA(1000)
      _      <- damper
        .resource
        .use { _ => IO.sleep(10.millis) }
        .replicateA(10)
        .parReplicateA(10)
      _      <- damper.acquire
      _      <- damper.release
      result <- last.get
      _      <- IO { result shouldEqual 0 }
      result <- max.get
      _      <- IO { result should be <= 100 }
      _      <- IO { result should be >= 10 }
    } yield {}
    result.run()
  }
}
