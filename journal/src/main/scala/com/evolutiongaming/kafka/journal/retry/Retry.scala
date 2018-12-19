package com.evolutiongaming.kafka.journal.retry

import cats._
import cats.effect.Timer
import cats.implicits._
import com.evolutiongaming.kafka.journal.util.{Rng, TimerOf}

import scala.concurrent.duration.FiniteDuration

object Retry {

  type Decide = Status => StrategyDecision

  def apply[F[_] : Sleep : FlatMap, A, E](
    strategy: Strategy,
    onError: (E, Details) => F[Unit])(
    call: F[A])(implicit
    applicativeError: ApplicativeError[F, E]): F[A] = {

    apply1(strategy, onError, call, applicativeError)
  }

  private def apply1[F[_] : Sleep : FlatMap, A, E](
    strategy: Strategy,
    onError: (E, Details) => F[Unit],
    call: F[A],
    applicativeError: ApplicativeError[F, E]): F[A] = {

    def apply(status: Status, decide: Decide): F[A] = {
      applicativeError.handleErrorWith(call) { e =>
        val decision = decide(status)
        val details = Details(decision, status.retries)
        for {
          _ <- onError(e, details)
          a <- decision match {
            case StrategyDecision.GiveUp               => applicativeError.raiseError(e)
            case StrategyDecision.Retry(delay, decide) => for {
              _ <- Sleep[F].apply(delay)
              a <- apply(status.inc, decide)
            } yield a
          }
        } yield a
      }
    }

    apply(Status.Empty, strategy.decide)
  }


  final case class Strategy(decide: Decide)

  object Strategy {

    def const(delay: FiniteDuration): Strategy = {
      def decide: Decide = (_: Status) => StrategyDecision.Retry(delay, decide)

      Strategy(decide)
    }

    def fibonacci(initial: FiniteDuration): Strategy = {
      val unit = initial.unit

      def apply(a: Long, b: Long): Decide = {
        _: Status => StrategyDecision.Retry(FiniteDuration(b, unit), apply(b, a + b))
      }

      Strategy(apply(0, initial.length))
    }

    def cap(max: FiniteDuration, strategy: Strategy): Strategy = {

      def apply(decide: Decide): Decide = {
        status: Status => {
          decide(status) match {
            case StrategyDecision.GiveUp               => StrategyDecision.GiveUp
            case StrategyDecision.Retry(delay, decide) =>
              if (delay <= max) StrategyDecision.Retry(delay, apply(decide))
              else StrategyDecision.Retry(max, const(max).decide)
          }
        }
      }

      Strategy(apply(strategy.decide))
    }

    /**
      * See https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
      */
    def fullJitter(initial: FiniteDuration, rng: Rng): Strategy = {

      def apply(rng: Rng): Decide = {
        status: Status => {
          val e = math.pow(2.toDouble, status.retries + 1d)
          val max = initial.length * e
          val (double, rng1) = rng.double
          val delay = (max * double).toLong max initial.length
          val duration = FiniteDuration(delay, initial.unit)
          StrategyDecision.Retry(duration, apply(rng1))
        }
      }

      Strategy(apply(rng))
    }
  }


  final case class Status(retries: Int) {
    def inc: Status = Status(retries = retries + 1)
  }

  object Status {
    val Empty: Status = Status(0)
  }


  final case class Details(decision: Decision, retries: Int)

  object Details {

    def apply(decision: Retry.StrategyDecision, retries: Int): Details = {
      val decision1 = decision match {
        case StrategyDecision.Retry(delay, _) => Decision.Retry(delay)
        case StrategyDecision.GiveUp          => Decision.GiveUp
      }
      apply(decision1, retries)
    }
  }


  sealed trait Decision

  object Decision {

    final case class Retry(delay: FiniteDuration) extends Decision

    case object GiveUp extends Decision
  }


  sealed trait StrategyDecision

  object StrategyDecision {

    final case class Retry(delay: FiniteDuration, decide: Decide) extends StrategyDecision

    case object GiveUp extends StrategyDecision
  }


  trait Sleep[F[_]] {

    def apply(duration: FiniteDuration): F[Unit]
  }

  object Sleep {

    def apply[F[_]](implicit F: Sleep[F]): Sleep[F] = F

    implicit def fromTimer[F[_] : Timer]: Sleep[F] = new Sleep[F] {
      def apply(duration: FiniteDuration) = TimerOf[F].sleep(duration)
    }
  }
}


