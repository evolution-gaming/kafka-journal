package com.evolutiongaming.kafka.journal.util

import java.util.concurrent.TimeUnit

import cats.effect.Clock

object ClockHelper {

  implicit class ClockOps[F[_]](val self: Clock[F]) extends AnyVal {

    def millis: F[Long] = self.realTime(TimeUnit.MILLISECONDS)

    def nanos: F[Long] = self.monotonic(TimeUnit.NANOSECONDS)
  }
}
