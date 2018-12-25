package com.evolutiongaming.kafka.journal.util

import cats.Applicative
import cats.effect.{Concurrent, ContextShift, Timer}
import cats.implicits._

import scala.concurrent.TimeoutException
import scala.concurrent.duration.FiniteDuration
import scala.util.control.NoStackTrace

object Timeout {

  def apply[F[_] : Timer : ContextShift : Concurrent, A](fa: F[A], after: FiniteDuration, fallback: F[A]): F[A] = {
    for {
      a <- Concurrent[F].race(fa, TimerOf[F].sleep(after))
      a <- a match {
        case Left(a)  => Applicative[F].pure(a)
        case Right(_) => fallback
      }
    } yield a
  }

  def apply[F[_] : Timer : ContextShift : Concurrent, A](fa: F[A], after: FiniteDuration): F[A] = {
    val error = new TimeoutException(after.toString) with NoStackTrace
    apply(fa, after, error.raiseError[F, A])
  }
}