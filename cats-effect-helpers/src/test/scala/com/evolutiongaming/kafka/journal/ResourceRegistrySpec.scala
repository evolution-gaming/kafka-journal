package com.evolutiongaming.kafka.journal

import cats.effect._
import cats.effect.concurrent.{Deferred, Ref}
import cats.implicits._
import cats.{Applicative, Foldable}
import com.evolutiongaming.kafka.journal.IOSuite._
import org.scalatest.{AsyncFunSuite, Matchers}

import scala.util.control.NoStackTrace

class ResourceRegistrySpec extends AsyncFunSuite with Matchers {

  val error: Throwable = new RuntimeException with NoStackTrace
  for {
    exitCase <- List(
      ExitCase.complete,
      ExitCase.error(error),
      ExitCase.canceled)
  } yield {

    test(s"ResRegistry releases resources, exitCase: $exitCase") {
      val result = exitCase match {
        case ExitCase.Completed    => testError(none)
        case ExitCase.Canceled     => testCancel
        case ExitCase.Error(error) => testError(error.some)
      }
      result.run()
    }
  }

  private def testError(error: Option[Throwable]) = {
    val n = 3

    def logic(release: IO[Unit]) = {
      ResourceRegistry.of[IO].use { registry =>
        val resource = Resource.make(().pure[IO]) { _ => release }
        val fa = registry.allocate(resource)
        implicit val monoidUnit = Applicative.monoid[IO, Unit]
        for {
          _ <- Foldable[List].fold(List.fill(n)(fa))
          _ <- error.fold(().pure[IO])(_.raiseError[IO, Unit])
        } yield {}
      }
    }

    for {
      ref      <- Ref.of[IO, Int](0)
      fa        = logic(ref.update(_ + 1))
      result   <- fa.redeem(_.some, _ => none)
      releases <- ref.get
    } yield {
      result shouldEqual result
      releases shouldEqual n
    }
  }

  private def testCancel = {
    for {
      released <- Ref.of[IO, Int](0)
      started  <- Deferred[IO, Unit]
      fiber    <- Concurrent[IO].start {
        ResourceRegistry.of[IO].use { registry =>
          val resource = Resource.make(().pure[IO]) { _ => released.update(_ + 1) }
          for {
            _ <- registry.allocate(resource)
            _ <- started.complete(())
            _ <- IO.never.as(())
          } yield {}
        }
      }
      _        <- started.get
      _        <- fiber.cancel
      released <- released.get
    } yield {
      released shouldEqual 1
    }
  }
}