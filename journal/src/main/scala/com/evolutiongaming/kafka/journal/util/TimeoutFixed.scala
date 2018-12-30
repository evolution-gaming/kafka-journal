package com.evolutiongaming.kafka.journal.util

import cats.effect.{Concurrent, Sync, Timer}
import cats.implicits._

import scala.concurrent.TimeoutException
import scala.concurrent.duration.FiniteDuration
import scala.util.control.NoStackTrace

// TODO remove when fixed in cats-effect
object TimeoutFixed {

  def apply[F[_] : Timer : Concurrent, A](fa: F[A], after: FiniteDuration): F[A] = {
    val fe = for {
      e <- Sync[F].delay { new TimeoutException(after.toString) with NoStackTrace }
      a <- Sync[F].raiseError[A](e)
    } yield a
    Concurrent.timeoutTo(fa, after, fe)
  }
}