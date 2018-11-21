package com.evolutiongaming.kafka.journal

import com.evolutiongaming.kafka.journal.IO.ops._

import scala.compat.Platform

object Latency {
  def apply[T, F[_] : IO](func: => F[T]): F[(T, Long)] = {
    val start = Platform.currentTime
    for {
      result <- func
      latency = Platform.currentTime - start
    } yield (result, latency)
  }
}