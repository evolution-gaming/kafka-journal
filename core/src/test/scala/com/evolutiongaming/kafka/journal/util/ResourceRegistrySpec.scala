package com.evolutiongaming.kafka.journal.util

import cats.effect.{Deferred, Ref, *}
import cats.syntax.all.*
import cats.{Applicative, Foldable}
import com.evolutiongaming.kafka.journal.IOSuite.*
import org.scalatest.funsuite.AsyncFunSuite
import org.scalatest.matchers.should.Matchers

import scala.util.control.NoStackTrace

class ResourceRegistrySpec extends AsyncFunSuite with Matchers {

  val error: Throwable = new RuntimeException with NoStackTrace
  for {
    exitCase <- List[Outcome[IO, Throwable, Unit]](Outcome.succeeded(().pure[IO]), Outcome.errored(error), Outcome.canceled)
  } yield {

    test(s"ResRegistry releases resources, exitCase: $exitCase") {
      testError(none).run()
      testCancel.run()
      testError(error.some).run()
      val result = exitCase match {
        case Outcome.Succeeded(_)   => testError(none)
        case Outcome.Canceled()     => testCancel
        case Outcome.Errored(error) => testError(error.some)
      }
      result.run()
    }
  }

  private def testError(error: Option[Throwable]) = {
    val n = 3

    def logic(release: IO[Unit]) = {
      ResourceRegistry.of[IO].use { registry =>
        val resource            = Resource.make(().pure[IO]) { _ => release }
        val fa                  = registry.allocate(resource)
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
      fiber <- Concurrent[IO].start {
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
