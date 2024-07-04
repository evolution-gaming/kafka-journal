package com.evolutiongaming.kafka.journal.util

import cats.effect.{Deferred, IO, Outcome}
import cats.syntax.all.*
import com.evolutiongaming.kafka.journal.IOSuite.*
import com.evolutiongaming.kafka.journal.util.CatsHelper.*
import org.scalatest.funsuite.AsyncFunSuite
import org.scalatest.matchers.should.Matchers

import scala.util.control.NoStackTrace

class CatsHelperTest extends AsyncFunSuite with Matchers {

  test("orElsePar") {
    val result = for {

      a <- 1.some.pure[IO].orElsePar(none[Int].pure[IO])
      _ <- IO { a shouldEqual 1.some }

      a <- none[Int].pure[IO].orElsePar(1.some.pure[IO])
      _ <- IO { a shouldEqual 1.some }

      a <- none[Int].pure[IO].orElsePar(none[Int].pure[IO])
      _ <- IO { a shouldEqual none }

      d0 <- Deferred[IO, Unit]
      d1 <- Deferred[IO, Option[Int]]
      d2 <- Deferred[IO, Outcome[IO, Throwable, Option[Int]]]
      a <- d1
        .get
        .orElsePar {
          d0
            .complete(())
            .productR { IO.never.as(1.some) }
            .guaranteeCase { a => d2.complete(a).void }
        }
        .start
      _ <- d0.get
      _ <- d1.complete(0.some)
      a <- a.join
      _ <- IO { a shouldEqual Outcome.succeeded(IO.pure(0.some)) }
      a <- d2.get
      _ <- IO { a shouldEqual Outcome.canceled }

      d0 <- Deferred[IO, Unit]
      d1 <- Deferred[IO, Either[Throwable, Option[Int]]]
      d2 <- Deferred[IO, Outcome[IO, Throwable, Option[Int]]]
      a <- d1
        .get
        .rethrow
        .orElsePar {
          d0
            .complete(())
            .productR { IO.never.as(1.some) }
            .guaranteeCase { a => d2.complete(a).void }
        }
        .start
      _ <- d0.get
      e  = new RuntimeException with NoStackTrace
      _ <- d1.complete(e.asLeft)
      a <- a.join.attempt
      _ <- IO { a shouldEqual Right(Outcome.errored(e)) }
      a <- d2.get
      _ <- IO { a shouldEqual Outcome.canceled }
    } yield {}
    result.run()
  }
}
