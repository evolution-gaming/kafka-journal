package com.evolution.kafka.journal.eventual.cassandra.util

import com.datastax.oss.driver.api.core.data.CqlDuration

import scala.concurrent.duration.*

object FiniteDurationHelper {

  def durationToFiniteDuration(a: CqlDuration): FiniteDuration = {
    (a.getDays.days + a.getNanoseconds.nanos).toCoarsest
  }

  def finiteDurationToDuration(a: FiniteDuration): CqlDuration = {
    val days = a.toDays
    val nanos = (a - days.days).toNanos
    CqlDuration.newInstance(0, days.toInt, nanos)
  }
}
