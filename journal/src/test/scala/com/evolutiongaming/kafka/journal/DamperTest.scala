package com.evolutiongaming.kafka.journal

import cats.effect.{FiberIO, IO, Ref}
import cats.effect.unsafe.implicits.global
import cats.syntax.all._
import org.scalatest.compatible.Assertion
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
      _      <- assertSleeping(fiber)
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
      _      <- assertSleeping(fiber0)
      fiber1 <- damper.acquire.start
      _      <- assertSleeping(fiber1)
      _      <- damper.release
      _      <- fiber0.joinWithNever
      _      <- fiber1.joinWithNever
    } yield {}
    result.run()
  }

  test("cancel") {
    val result = for {
      // start -> sleep(1.minute) -> fiber0 -> sleep(0.minutes) -> fiber1
      ref    <- Ref[IO].of(List(1.minute, 0.minutes))
      damper <- Damper.of[IO] { _ =>
        ref
          .modify {
            // always return 1.minute
            // after two delays above from initial Ref are consumed
            case Nil     => (Nil, 1.minute)
            case a :: as => (as, a)
          }
          .unsafeRunSync()
      }

      fiber0 <- damper.acquire.start
      _      <- assertSleeping(fiber0)

      fiber1 <- damper.acquire.start
      _      <- assertSleeping(fiber1)

      fiber2 <- damper.acquire.start
      _      <- assertSleeping(fiber2)

      _      <- fiber1.cancel
      _      <- fiber0.cancel

      delays <- ref.get
      _      <- IO {
        assert(
          delays.nonEmpty,
          "delayOf was called more than once, the test result will not be meaningful," +
          "consider increasing sleeping delay in `assertSleeping` to ensure the orderly" +
          "start of fiber0, fiber1 and fiber2")
      }

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

  def assertSleeping(fiber: FiberIO[Unit]): IO[Assertion] =
    fiber.joinWithNever.timeout(100.millis).attempt flatMap { result =>
      IO { result should matchPattern { case Left(_: TimeoutException) => () } }
    }

}
