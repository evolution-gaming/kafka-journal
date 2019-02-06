package com.evolutiongaming.kafka.journal

import cats.effect.Sync
import cats.implicits._

import scala.concurrent.TimeoutException
import scala.util.control.NoStackTrace

final case class TimeoutError(msg: String) extends TimeoutException(msg) with NoStackTrace

object TimeoutError {

  def lift[F[_] : Sync, A](msg: String): F[A] = {
    for {
      e <- Sync[F].delay { TimeoutError(msg) }
      a <- Sync[F].raiseError[A](e)
    } yield a
  }
}
