package com.evolutiongaming.kafka.journal.retry

import java.time.Instant

import cats.effect.{Bracket, Clock, Timer}
import cats.implicits._
import com.evolutiongaming.kafka.journal.CatsHelper._
import com.evolutiongaming.kafka.journal.ClockHelper._
import com.evolutiongaming.kafka.journal.rng.Rng

import scala.concurrent.duration._

trait Retry[F[_]] {

  def apply[A](fa: F[A]): F[A]
}

object Retry {

  def apply[F[_]](implicit F: Retry[F]): Retry[F] = F


  def apply[F[_] : Timer, E](
    strategy: Strategy)(
    onError: (E, Details) => F[Unit])(implicit
    bracket: Bracket[F, E]
  ): Retry[F] = {

    type S = (Status, Decide)

    def retry[A](status: Status, decide: Decide, error: E): F[Either[S, A]] = {

      def onError1(status: Status, decision: StrategyDecision) = {
        val details = Details(decision, status.retries)
        onError(error, details)
      }

      for {
        now      <- Clock[F].instant
        decision  = decide(status, now)
        result   <- decision match {
          case StrategyDecision.GiveUp =>
            for {
              _      <- onError1(status, decision)
              result <- error.raiseError[F, Either[S, A]]
            } yield result

          case StrategyDecision.Retry(delay, status, decide) =>
            for {
              _ <- onError1(status, decision)
              _ <- Timer[F].sleep(delay)
            } yield {
              (status.plus(delay), decide).asLeft[A]
            }
        }
      } yield result
    }

    new Retry[F] {

      def apply[A](fa: F[A]) = {
        for {
          now    <- Clock[F].instant
          zero    = (Status.empty(now), strategy.decide)
          result <- zero.tailRecM[F, A] { case (status, decide) =>
            fa.redeemWith { error =>
              retry[A](status, decide, error)
            } { result =>
              result.asRight[(Status, Decide)].pure[F]
            }
          }
        } yield result
      }
    }
  }


  def empty[F[_]]: Retry[F] = new Retry[F] {
    def apply[A](fa: F[A]) = fa
  }


  trait Decide {
    def apply(status: Status, now: Instant): StrategyDecision
  }

  object Decide {

    // TODO rename
    def tmp(decision: StrategyDecision): Decide = new Decide {
      def apply(status: Status, now: Instant): StrategyDecision = decision
    }
  }


  final case class Strategy(decide: Decide)

  object Strategy {

    def const(delay: FiniteDuration): Strategy = {
      def decide: Decide = new Decide {
        def apply(status: Status, now: Instant) = {
          StrategyDecision.retry(delay, status, decide)
        }
      }

      Strategy(decide)
    }


    def fibonacci(initial: FiniteDuration): Strategy = {
      val unit = initial.unit

      def recur(a: Long, b: Long): Decide = new Decide {

        def apply(status: Status, now: Instant) = {
          val delay = FiniteDuration(b, unit)
          StrategyDecision.retry(delay, status, recur(b, a + b))
        }
      }

      Strategy(recur(0, initial.length))
    }


    /**
      * See https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
      */
    def fullJitter(initial: FiniteDuration, rng: Rng): Strategy = {

      def recur(rng: Rng): Decide = new Decide {

        def apply(status: Status, now: Instant) = {
          val e = math.pow(2.toDouble, status.retries + 1d)
          val max = initial.length * e
          val (double, rng1) = rng.double
          val delay = (max * double).toLong max initial.length
          val duration = FiniteDuration(delay, initial.unit)
          StrategyDecision.retry(duration, status, recur(rng1))
        }
      }

      Strategy(recur(rng))
    }


    def cap(strategy: Strategy, max: FiniteDuration): Strategy = {

      def recur(decide: Decide): Decide = new Decide {

        def apply(status: Status, now: Instant) = {
          decide(status, now) match {
            case StrategyDecision.GiveUp                       => StrategyDecision.giveUp
            case StrategyDecision.Retry(delay, status, decide) =>
              if (delay <= max) StrategyDecision.retry(delay, status, recur(decide))
              else StrategyDecision.retry(max, status, const(max).decide)
          }
        }
      }

      Strategy(recur(strategy.decide))
    }


    def limit(strategy: Strategy, max: FiniteDuration): Strategy = {

      def recur(decide: Decide): Decide = new Decide {

        def apply(status: Status, now: Instant) = {
          decide(status, now) match {
            case StrategyDecision.GiveUp                       => StrategyDecision.giveUp
            case StrategyDecision.Retry(delay, status, decide) =>
              if (status.delay + delay > max) StrategyDecision.giveUp
              else StrategyDecision.retry(delay, status, recur(decide))
          }
        }
      }

      Strategy(recur(strategy.decide))
    }


    def resetAfter(strategy: Strategy, cooldown: FiniteDuration): Strategy = {

      def recur(decide: Decide): Decide = new Decide {

        def apply(status: Status, now: Instant) = {

          val reset = status.last.toEpochMilli + cooldown.toMillis <= now.toEpochMilli

          val result = {
            if (reset) {
              val status = Status.empty(now)
              strategy.decide(status, now)
            } else {
              decide(status, now)
            }
          }
          result.mapDecide(recur)
        }
      }

      Strategy(recur(strategy.decide))
    }
  }


  final case class Status(retries: Int, delay: FiniteDuration, last: Instant) { self =>

    def plus(delay: FiniteDuration): Status = {
      copy(retries = retries + 1, delay = self.delay + delay)
    }
  }

  object Status {
    def empty(last: Instant): Status = Status(0, Duration.Zero, last)
  }


  final case class Details(decision: Decision, retries: Int /*, delay: FiniteDuration*/)

  object Details {

    def apply(decision: Retry.StrategyDecision, retries: Int): Details = {
      val decision1 = decision match {
        case StrategyDecision.Retry(delay, _, _) => Decision.retry(delay)
        case StrategyDecision.GiveUp             => Decision.giveUp
      }
      apply(decision1, retries)
    }
  }


  sealed abstract class Decision extends Product

  object Decision {

    def retry(delay: FiniteDuration): Decision = Retry(delay)

    def giveUp: Decision = GiveUp


    final case class Retry(delay: FiniteDuration) extends Decision

    case object GiveUp extends Decision
  }


  sealed abstract class StrategyDecision extends Product

  object StrategyDecision {

    def retry(delay: FiniteDuration, status: Status, decide: Decide): StrategyDecision = {
      Retry(delay, status, decide)
    }

    def giveUp: StrategyDecision = GiveUp


    final case class Retry(delay: FiniteDuration, status: Status, decide: Decide) extends StrategyDecision

    case object GiveUp extends StrategyDecision


    implicit class StrategyDecisionOps(val self: StrategyDecision) extends AnyVal {

      def mapDecide(f: Decide => Decide): StrategyDecision = {
        self match {
          case StrategyDecision.GiveUp   => StrategyDecision.giveUp
          case a: StrategyDecision.Retry => a.copy(decide = f(a.decide))
        }
      }
    }
  }


  implicit class StrategyOps(val self: Strategy) extends AnyVal {

    def cap(max: FiniteDuration): Strategy = Strategy.cap(self, max)

    def limit(max: FiniteDuration): Strategy = Strategy.limit(self, max)

    def resetAfter(cooldown: FiniteDuration): Strategy = Strategy.resetAfter(self, cooldown)
  }
}


