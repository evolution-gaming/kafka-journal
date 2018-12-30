package com.evolutiongaming.kafka.journal.retry

import cats.effect.{Bracket, Timer}
import cats.implicits._
import com.evolutiongaming.kafka.journal.util.CatsHelper._
import com.evolutiongaming.kafka.journal.util.Rng

import scala.concurrent.duration.FiniteDuration

object Retry {

  type Decide = Status => StrategyDecision

  def apply[F[_] : Sleep, A, E](
    strategy: Strategy,
    onError: (E, Details) => F[Unit])(
    call: F[A])(implicit
    bracket: Bracket[F, E]): F[A] = {

    type S = (Status, Decide)

    (Status.Empty, strategy.decide).tailRecM[F, A] { case (status, decide) =>

      def retry(e: E): F[Either[S, A]] = {
        val decision = decide(status)
        val details = Details(decision, status.retries)
        for {
          _ <- onError(e, details)
          a <- decision match {
            case StrategyDecision.GiveUp =>
              for {a <- e.raiseError[F, A]} yield a.asRight[S]

            case StrategyDecision.Retry(delay, decide) =>
              for {_ <- Sleep[F].apply(delay)} yield (status.inc, decide).asLeft[A]
          }
        } yield a
      }

      call.redeemWith(retry)(_.asRight[(Status, Decide)].pure[F])
    }
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
      def apply(duration: FiniteDuration) = Timer[F].sleep(duration)
    }
  }
}


