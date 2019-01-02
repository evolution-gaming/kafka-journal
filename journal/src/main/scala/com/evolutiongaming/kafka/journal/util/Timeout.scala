package com.evolutiongaming.kafka.journal.util

import cats.effect.{Concurrent, Timer}
import com.evolutiongaming.kafka.journal.util.CatsHelper.ConcurrentOps

import scala.concurrent.duration.FiniteDuration

// TODO use in timeout1
// TODO do we need this?
trait Timeout[F[_]] {
  def apply[A](fa: F[A], duration: FiniteDuration): F[A]
}

object Timeout {

  def apply[F[_]](implicit F: Timeout[F]): Timeout[F] = F

  def lift[F[_] : Concurrent : Timer]: Timeout[F] = {
    new Timeout[F] {
      def apply[A](fa: F[A], duration: FiniteDuration) = Concurrent.timeout1(fa, duration)
    }
  }
}
