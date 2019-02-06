package com.evolutiongaming.kafka.journal

import cats.effect.{Concurrent, Timer}

import scala.concurrent.duration.FiniteDuration

// TODO remove when fixed in cats-effect
object TimeoutFixed {

  def apply[F[_] : Timer : Concurrent, A](fa: F[A], after: FiniteDuration): F[A] = {
    val fe = TimeoutError.lift[F, A](s"timed out after $after")
    Concurrent.timeoutTo(fa, after, fe)
  }
}