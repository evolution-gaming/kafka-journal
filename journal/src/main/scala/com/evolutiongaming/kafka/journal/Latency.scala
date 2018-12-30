package com.evolutiongaming.kafka.journal

import cats.FlatMap
import cats.implicits._
import cats.effect.Clock
import com.evolutiongaming.kafka.journal.util.ClockHelper._

object Latency {

  def apply[F[_] : FlatMap : Clock, A](func: => F[A] /*TODO change*/): F[(A, Long)] = {
    for {
      start   <- Clock[F].millis
      result  <- func
      end     <- Clock[F].millis
      latency  = end - start
    } yield {
      (result, latency)
    }
  }
}