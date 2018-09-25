package com.evolutiongaming.kafka.journal

import com.evolutiongaming.kafka.journal.FlatMap._

import scala.compat.Platform

object Latency {
  def apply[T, F[_]](func: => F[T])(implicit flatMap: FlatMap[F]): F[(T, Long)] = {
    val start = Platform.currentTime
    for {
      result <- func
      latency = Platform.currentTime - start
    } yield (result, latency)
  }
}