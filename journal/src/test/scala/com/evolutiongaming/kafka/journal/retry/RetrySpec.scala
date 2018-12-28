package com.evolutiongaming.kafka.journal.retry

import cats._
import cats.data._
import cats.effect.{Bracket, ExitCase}
import cats.implicits._
import com.evolutiongaming.kafka.journal.retry.Retry._
import com.evolutiongaming.kafka.journal.util.Rng
import org.scalatest.{FunSuite, Matchers}

import scala.annotation.tailrec
import scala.concurrent.duration._

class RetrySpec extends FunSuite with Matchers {
  import RetrySpec.DataState._
  import RetrySpec._

  test("fibonacci") {
    val policy = {
      val strategy = Strategy.fibonacci(5.millis)
      Strategy.cap(200.millis, strategy)
    }

    val call = DataState { _.call }
    val result = Retry(policy, onError)(call)

    val initial = Data(toRetry = 10)
    val actual = result.run(initial).map(_._1)
    val expected = Data(
      decisions = List(
        Details(decision = Decision.Retry(200.millis), retries = 9),
        Details(decision = Decision.Retry(170.millis), retries = 8),
        Details(decision = Decision.Retry(105.millis), retries = 7),
        Details(decision = Decision.Retry(65.millis), retries = 6),
        Details(decision = Decision.Retry(40.millis), retries = 5),
        Details(decision = Decision.Retry(25.millis), retries = 4),
        Details(decision = Decision.Retry(15.millis), retries = 3),
        Details(decision = Decision.Retry(10.millis), retries = 2),
        Details(decision = Decision.Retry(5.millis), retries = 1),
        Details(decision = Decision.Retry(5.millis), retries = 0)),
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
    val policy = {
      val rng = Rng(12345l)
      val strategy = Strategy.fullJitter(5.millis, rng)
      Strategy.cap(200.millis, strategy)
    }

    val call = DataState { _.call }
    val result = Retry(policy, onError)(call)

    val initial = Data(toRetry = 7)
    val actual = result.run(initial).map(_._1)
    val expected = Data(
      decisions = List(
        Details(decision = Decision.Retry(200.millis), retries = 6),
        Details(decision = Decision.Retry(133.millis), retries = 5),
        Details(decision = Decision.Retry(34.millis), retries = 4),
        Details(decision = Decision.Retry(79.millis), retries = 3),
        Details(decision = Decision.Retry(26.millis), retries = 2),
        Details(decision = Decision.Retry(15.millis), retries = 1),
        Details(decision = Decision.Retry(5.millis), retries = 0)),
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
}

object RetrySpec {

  type Error = Unit

  type FE[A] = Either[Error, A]

  val onError: (Error, Details) => DataState[Error] = (_: Error, details: Details) => {
    DataState { s => (s.onError(details), ().asRight) }
  }

  type DataState[A] = StateT[Id, Data, FE[A]]

  object DataState {

    implicit val BracketImpl: Bracket[DataState, Error] = new Bracket[DataState, Error] {

      def flatMap[A, B](fa: DataState[A])(f: A => DataState[B]) = {
        DataState[B] { s =>
          val (s1, a) = fa.run(s)
          a.fold(a => (s1, a.asLeft), a => f(a).run(s1))
        }
      }

      def tailRecM[A, B](a: A)(f: A => DataState[Either[A, B]]) = {

        @tailrec
        def apply(s: Data, a: A): (Data, FE[B]) = {
          val (s1, b) = f(a).run(s)
          b match {
            case Right(Right(b)) => (s1, b.asRight)
            case Right(Left(b))  => apply(s1, b)
            case Left(b)         => (s1, b.asLeft)
          }
        }

        DataState { s => apply(s, a) }
      }

      def bracketCase[A, B](acquire: DataState[A])(use: A => DataState[B])(release: (A, ExitCase[Error]) => DataState[Error]) = {
        DataState { s =>
          val (s1, a) = acquire.run(s)
          a match {
            case Left(a)  => (s1, a.asLeft[B])
            case Right(a) =>
              val (s2, b) = use(a).run(s1)
              val exitCase = b.fold(ExitCase.error, _ => ExitCase.complete)
              release(a, exitCase)
              (s2, b)
          }
        }
      }

      def raiseError[A](e: Error) = {
        DataState { s => (s, e.asLeft) }
      }

      def handleErrorWith[A](fa: DataState[A])(f: Error => DataState[A]) = {
        DataState { s =>
          val (s1, a) = fa.run(s)
          a.fold(a => f(a).run(s1), a => (s1, a.asRight))
        }
      }

      def pure[A](a: A) = DataState { s => (s, a.asRight) }
    }


    implicit val SleepImpl: Sleep[DataState] = new Sleep[DataState] {

      def apply(duration: FiniteDuration) = {
        DataState { s => (s.sleep(duration), ().asRight) }
      }
    }

    def apply[A](f: Data => (Data, FE[A])): DataState[A] = {
      StateT[Id, Data, FE[A]](f)
    }
  }


  final case class Data(
    toRetry: Int = 0,
    decisions: List[Details] = Nil,
    delays: List[FiniteDuration] = Nil) { self =>

    def sleep(duration: FiniteDuration): Data = {
      copy(delays = duration :: delays)
    }

    def call: (Data, FE[Unit]) = {
      if (toRetry > 0) {
        (copy(toRetry = toRetry - 1), ().asLeft)
      } else {
        (self, ().asRight)
      }
    }

    def onError(details: Details): Data = copy(decisions = details :: self.decisions)
  }
}
