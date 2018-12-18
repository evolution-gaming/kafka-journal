package com.evolutiongaming.kafka.journal.retry

import cats._
import cats.effect.Timer
import cats.implicits._
import com.evolutiongaming.kafka.journal.util.TimerOf

import scala.concurrent.duration.FiniteDuration

object Retry {

  type Decide = Status => PolicyDecision

  def apply[F[_] : Sleep : FlatMap, A, E](
    policy: Policy,
    onError: (E, Details) => F[Unit])(
    call: F[A])(implicit
    applicativeError: ApplicativeError[F, E]): F[A] = {

    apply1(policy, onError, call, applicativeError)
  }

  private def apply1[F[_] : Sleep : FlatMap, A, E](
    policy: Policy,
    onError: (E, Details) => F[Unit],
    call: F[A],
    applicativeError: ApplicativeError[F, E]): F[A] = {

    def apply(status: Status, decide: Decide): F[A] = {
      applicativeError.handleErrorWith(call) { e =>
        val decision = decide(status)
        val details = Details(decision, status.retries)
        for {
          _ <- onError(e, details)
          a <- decide(status) match {
            case PolicyDecision.GiveUp               => applicativeError.raiseError(e)
            case PolicyDecision.Retry(delay, decide) => for {
              _ <- Sleep[F].apply(delay)
              a <- apply(status.inc, decide)
            } yield a
          }
        } yield a
      }
    }

    apply(Status.Empty, policy.decide)
  }


  final case class Policy(decide: Decide)

  object Policy {

    def const(delay: FiniteDuration): Policy = {
      def decide: Decide = (_: Status) => PolicyDecision.Retry(delay, decide)

      Policy(decide)
    }

    def fibonacci(delay: FiniteDuration): Policy = {
      val unit = delay.unit

      def apply(a: Long, b: Long): Decide = {
        _: Status => PolicyDecision.Retry(FiniteDuration(b, unit), apply(b, a + b))
      }

      Policy(apply(0, delay.length))
    }

    def cap(max: FiniteDuration, policy: Policy): Policy = {

      def apply(decide: Decide): Decide = {
        status: Status =>
          decide(status) match {
            case PolicyDecision.GiveUp               => PolicyDecision.GiveUp
            case PolicyDecision.Retry(delay, decide) =>
              if (delay <= max) PolicyDecision.Retry(delay, apply(decide))
              else PolicyDecision.Retry(max, const(max).decide)
          }
      }

      Policy(apply(policy.decide))
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

    def apply(decision: Retry.PolicyDecision, retries: Int): Details = {
      val decision1 = decision match {
        case PolicyDecision.Retry(delay, _) => Decision.Retry(delay)
        case PolicyDecision.GiveUp          => Decision.GiveUp
      }
      apply(decision1, retries)
    }
  }


  sealed trait Decision

  object Decision {

    final case class Retry(delay: FiniteDuration) extends Decision

    case object GiveUp extends Decision
  }


  sealed trait PolicyDecision

  object PolicyDecision {

    final case class Retry(delay: FiniteDuration, decide: Decide) extends PolicyDecision

    case object GiveUp extends PolicyDecision
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


