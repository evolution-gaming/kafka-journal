package com.evolutiongaming.kafka.journal

import com.evolutiongaming.kafka.journal.IO2.ops._

import scala.compat.Platform

object Latency {
  def apply[T, F[_] : IO2](func: => F[T]): F[(T, Long)] = {
    val start = Platform.currentTime
    for {
      result <- func
      latency = Platform.currentTime - start
    } yield (result, latency)
  }
}