package com.evolutiongaming.kafka.journal.util

import cats.effect.IO

import scala.concurrent.Future

object IOFromFuture {

  def apply[A](a: => Future[A]): IO[A] = {
    val future = IO.delay(a)
    IO.fromFuture(future)
  }
}
