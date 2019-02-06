package com.evolutiongaming.kafka.journal

import cats.FlatMap
import cats.implicits._
import cats.effect.Clock
import com.evolutiongaming.kafka.journal.ClockHelper._

object Latency {

  def apply[F[_] : FlatMap : Clock, A](fa: F[A]): F[(A, Long)] = {
    for {
      start <- Clock[F].millis
      a     <- fa
      end   <- Clock[F].millis
    } yield {
      val latency = end - start
      (a, latency)
    }
  }
}