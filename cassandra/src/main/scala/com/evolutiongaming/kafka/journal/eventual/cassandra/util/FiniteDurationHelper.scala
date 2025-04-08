package com.evolutiongaming.kafka.journal.eventual.cassandra.util

import com.datastax.driver.core.Duration as DurationC

import scala.concurrent.duration.*

object FiniteDurationHelper {

  def durationToFiniteDuration(a: DurationC): FiniteDuration = {
    (a.getDays.days + a.getNanoseconds.nanos).toCoarsest
  }

  def finiteDurationToDuration(a: FiniteDuration): DurationC = {
    val days = a.toDays
    val nanos = (a - days.days).toNanos
    DurationC.newInstance(0, days.toInt, nanos)
  }
}
