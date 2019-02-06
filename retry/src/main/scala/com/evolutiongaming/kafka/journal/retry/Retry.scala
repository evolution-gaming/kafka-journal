package com.evolutiongaming.kafka.journal.retry

import cats.effect.{Bracket, Timer}
import cats.implicits._
import com.evolutiongaming.kafka.journal.CatsHelper._
import com.evolutiongaming.kafka.journal.rng.Rng

import scala.concurrent.duration._

trait Retry[F[_]] {

  def apply[A](fa: F[A]): F[A]
}

object Retry {

  type Decide = Status => StrategyDecision


  def apply[F[_] : Sleep, E](
    strategy: Strategy)(
    onError: (E, Details) => F[Unit])(implicit bracket: Bracket[F, E]): Retry[F] = {

    type S = (Status, Decide)

    new Retry[F] {
      def apply[A](fa: F[A]) = {

        (Status.Empty, strategy.decide).tailRecM[F, A] { case (status, decide) =>

          def retry(e: E): F[Either[S, A]] = {
            val decision = decide(status)
            val details = Details(decision, status.retries)
            for {
              _ <- onError(e, details)
              a <- decision match {
                case StrategyDecision.GiveUp =>
                  e.raiseError[F, Either[S, A]]

                case StrategyDecision.Retry(delay, decide) =>
                  for {
                    _ <- Sleep[F].apply(delay)
                  } yield {
                    (status.plus(delay), decide).asLeft[A]
                  }
              }
            } yield a
          }

          fa.redeemWith(retry)(_.asRight[(Status, Decide)].pure[F])
        }
      }
    }
  }


  final case class Strategy(decide: Decide)

  object Strategy {

    def const(delay: FiniteDuration): Strategy = {
      def decide: Decide = (_: Status) => StrategyDecision.retry(delay, decide)

      Strategy(decide)
    }

    def fibonacci(initial: FiniteDuration): Strategy = {
      val unit = initial.unit

      def apply(a: Long, b: Long): Decide = {
        _: Status => StrategyDecision.retry(FiniteDuration(b, unit), apply(b, a + b))
      }

      Strategy(apply(0, initial.length))
    }

    def cap(max: FiniteDuration, strategy: Strategy): Strategy = {

      def apply(decide: Decide): Decide = {
        status: Status => {
          decide(status) match {
            case StrategyDecision.GiveUp               => StrategyDecision.giveUp
            case StrategyDecision.Retry(delay, decide) =>
              if (delay <= max) StrategyDecision.retry(delay, apply(decide))
              else StrategyDecision.retry(max, const(max).decide)
          }
        }
      }

      Strategy(apply(strategy.decide))
    }

    def limit(max: FiniteDuration, strategy: Strategy): Strategy = {

      def apply(decide: Decide): Decide = {
        status: Status => {
          decide(status) match {
            case StrategyDecision.GiveUp               => StrategyDecision.giveUp
            case StrategyDecision.Retry(delay, decide) =>
              if (status.delay + delay > max) StrategyDecision.giveUp
              else StrategyDecision.retry(delay, apply(decide))
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
          StrategyDecision.retry(duration, apply(rng1))
        }
      }

      Strategy(apply(rng))
    }
  }


  final case class Status(retries: Int, delay: FiniteDuration) { self =>

    def plus(delay: FiniteDuration): Status = {
      copy(retries = retries + 1, delay = self.delay + delay)
    }
  }

  object Status {
    val Empty: Status = Status(0, Duration.Zero)
  }


  final case class Details(decision: Decision, retries: Int /*, delay: FiniteDuration*/)

  object Details {

    def apply(decision: Retry.StrategyDecision, retries: Int): Details = {
      val decision1 = decision match {
        case StrategyDecision.Retry(delay, _) => Decision.retry(delay)
        case StrategyDecision.GiveUp          => Decision.giveUp
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

    def retry(delay: FiniteDuration, decide: Decide): StrategyDecision = Retry(delay, decide)

    def giveUp: StrategyDecision = GiveUp


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


  implicit class StrategyOps(val self: Strategy) extends AnyVal {

    def cap(max: FiniteDuration): Strategy = Strategy.cap(max, self)

    def limit(max: FiniteDuration): Strategy = Strategy.limit(max, self)
  }
}


