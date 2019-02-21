package com.evolutiongaming.kafka.journal.retry

import cats._
import cats.effect.{Bracket, ExitCase, Timer}
import cats.implicits._
import com.evolutiongaming.kafka.journal.ClockOf
import com.evolutiongaming.kafka.journal.retry.Retry._
import com.evolutiongaming.kafka.journal.rng.Rng
import org.scalatest.{FunSuite, Matchers}

import scala.annotation.tailrec
import scala.compat.Platform
import scala.concurrent.duration._

class RetrySpec extends FunSuite with Matchers {
  import RetrySpec.StateT._
  import RetrySpec._

  test("fibonacci") {
    val strategy = Strategy.fibonacci(5.millis).cap(200.millis)

    val call = StateT { _.call }
    val result = Retry(strategy)(onError).apply(call)

    val initial = State(toRetry = 10)
    val actual = result.run(initial).map(_._1)
    val expected = State(
      decisions = List(
        Details(decision = Decision.retry(200.millis), retries = 9),
        Details(decision = Decision.retry(170.millis), retries = 8),
        Details(decision = Decision.retry(105.millis), retries = 7),
        Details(decision = Decision.retry(65.millis), retries = 6),
        Details(decision = Decision.retry(40.millis), retries = 5),
        Details(decision = Decision.retry(25.millis), retries = 4),
        Details(decision = Decision.retry(15.millis), retries = 3),
        Details(decision = Decision.retry(10.millis), retries = 2),
        Details(decision = Decision.retry(5.millis), retries = 1),
        Details(decision = Decision.retry(5.millis), retries = 0)),
      delays = List(
        200.millis,
        170.millis,
        105.millis,
        65.millis,
        40.millis,
        25.millis,
        15.millis,
        10.millis,
        5.millis,
        5.millis))
    actual shouldEqual expected
  }

  test("fullJitter") {
    val rng = Rng(12345l)
    val policy = Strategy.fullJitter(5.millis, rng).cap(200.millis)

    val call = StateT { _.call }
    val result = Retry(policy)(onError).apply(call)

    val initial = State(toRetry = 7)
    val actual = result.run(initial).map(_._1)
    val expected = State(
      decisions = List(
        Details(decision = Decision.retry(200.millis), retries = 6),
        Details(decision = Decision.retry(133.millis), retries = 5),
        Details(decision = Decision.retry(34.millis), retries = 4),
        Details(decision = Decision.retry(79.millis), retries = 3),
        Details(decision = Decision.retry(26.millis), retries = 2),
        Details(decision = Decision.retry(15.millis), retries = 1),
        Details(decision = Decision.retry(5.millis), retries = 0)),
      delays = List(
        200.millis,
        133.millis,
        34.millis,
        79.millis,
        26.millis,
        15.millis,
        5.millis))
    actual shouldEqual expected
  }

  test("const") {
    val strategy = Strategy.const(1.millis).limit(4.millis)
    val call = StateT { _.call }
    val result = Retry(strategy)(onError).apply(call)

    val initial = State(toRetry = 6)
    val actual = result.run(initial).map(_._1)
    val expected = State(
      toRetry = 1,
      decisions = List(
        Details(decision = Decision.giveUp, retries = 4),
        Details(decision = Decision.retry(1.millis), retries = 3),
        Details(decision = Decision.retry(1.millis), retries = 2),
        Details(decision = Decision.retry(1.millis), retries = 1),
        Details(decision = Decision.retry(1.millis), retries = 0)),
      delays = List(
        1.millis,
        1.millis,
        1.millis,
        1.millis))
    actual shouldEqual expected
  }

  test("resetAfter 0.millis") {
    val strategy = Strategy.fibonacci(5.millis).resetAfter(0.millis)

    val call = StateT { _.call }
    val result = Retry(strategy)(onError).apply(call)

    val initial = State(toRetry = 3)
    val actual = result.run(initial).map(_._1)
    val expected = State(
      decisions = List(
        Details(decision = Decision.retry(5.millis), retries = 0),
        Details(decision = Decision.retry(5.millis), retries = 0),
        Details(decision = Decision.retry(5.millis), retries = 0)),
      delays = List(
        5.millis,
        5.millis,
        5.millis))
    actual shouldEqual expected
  }

  test("resetAfter 1.minute") {
    val strategy = Strategy.fibonacci(5.millis).resetAfter(1.minute)

    val call = StateT { _.call }
    val result = Retry(strategy)(onError).apply(call)

    val initial = State(toRetry = 3)
    val actual = result.run(initial).map(_._1)
    val expected = State(
      decisions = List(
        Details(decision = Decision.retry(10.millis), retries = 2),
        Details(decision = Decision.retry(5.millis), retries = 1),
        Details(decision = Decision.retry(5.millis), retries = 0)),
      delays = List(
        10.millis,
        5.millis,
        5.millis))
    actual shouldEqual expected
  }
}

object RetrySpec {

  type Error = Unit

  type FE[A] = Either[Error, A]

  val onError: (Error, Details) => StateT[Error] = (_: Error, details: Details) => {
    StateT { s => (s.onError(details), ().asRight) }
  }

  type StateT[A] = cats.data.StateT[Id, State, FE[A]]

  object StateT {

    implicit val BracketImpl: Bracket[StateT, Error] = new Bracket[StateT, Error] {

      def flatMap[A, B](fa: StateT[A])(f: A => StateT[B]) = {
        StateT[B] { s =>
          val (s1, a) = fa.run(s)
          a.fold(a => (s1, a.asLeft), a => f(a).run(s1))
        }
      }

      def tailRecM[A, B](a: A)(f: A => StateT[Either[A, B]]) = {

        @tailrec
        def apply(s: State, a: A): (State, FE[B]) = {
          val (s1, b) = f(a).run(s)
          b match {
            case Right(Right(b)) => (s1, b.asRight)
            case Right(Left(b))  => apply(s1, b)
            case Left(b)         => (s1, b.asLeft)
          }
        }

        StateT { s => apply(s, a) }
      }

      def bracketCase[A, B](acquire: StateT[A])(use: A => StateT[B])(release: (A, ExitCase[Error]) => StateT[Error]) = {
        StateT { s =>
          val (s1, a) = acquire.run(s)
          a match {
            case Left(a)  => (s1, a.asLeft[B])
            case Right(a) =>
              val (s2, b) = use(a).run(s1)
              val exitCase = b.fold(ExitCase.error, _ => ExitCase.complete)
              val(s3, _) = release(a, exitCase).run(s2)
              (s3, b)
          }
        }
      }

      def raiseError[A](e: Error) = {
        StateT { s => (s, e.asLeft) }
      }

      def handleErrorWith[A](fa: StateT[A])(f: Error => StateT[A]) = {
        StateT { s =>
          val (s1, a) = fa.run(s)
          a.fold(a => f(a).run(s1), a => (s1, a.asRight))
        }
      }

      def pure[A](a: A) = StateT { s => (s, a.asRight) }
    }


    implicit val TimerStateT: Timer[StateT] = new Timer[StateT] {

      val clock = ClockOf[StateT](Platform.currentTime)

      def sleep(duration: FiniteDuration) = {
        StateT { s => (s.sleep(duration), ().asRight) }
      }
    }

    def apply[A](f: State => (State, FE[A])): StateT[A] = {
      cats.data.StateT[Id, State, FE[A]](f)
    }
  }


  final case class State(
    toRetry: Int = 0,
    decisions: List[Details] = Nil,
    delays: List[FiniteDuration] = Nil
  ) { self =>

    def sleep(duration: FiniteDuration): State = {
      copy(delays = duration :: delays)
    }

    def call: (State, FE[Unit]) = {
      if (toRetry > 0) {
        (copy(toRetry = toRetry - 1), ().asLeft)
      } else {
        (self, ().asRight)
      }
    }

    def onError(details: Details): State = copy(decisions = details :: self.decisions)
  }
}
