package com.evolutiongaming.kafka.journal.util

import java.time.Instant
import java.time.temporal.{ChronoUnit, Temporal, TemporalAmount}
import java.util.concurrent.TimeUnit

import cats.Order

import scala.concurrent.duration._

object TimeHelper {

  implicit val InstantOrdering: Ordering[Instant] = Ordering.fromLessThan(_ isBefore _)

  implicit val InstantOrder: Order[Instant] = Order.fromLessThan(_ isBefore _)


  implicit class TemporalOps[T <: Temporal](val self: T) extends AnyVal {

    def +(duration: TemporalAmount): T = (self plus duration).asInstanceOf[T]

    def +(duration: FiniteDuration): T = {
      self.plus(duration.length, duration.unit.chronoUnit).asInstanceOf[T]
    }

    def -(duration: TemporalAmount): T = (self minus duration).asInstanceOf[T]

    def -(duration: FiniteDuration): T = {
      self.minus(duration.length, duration.unit.chronoUnit).asInstanceOf[T]
    }
  }


  implicit class InstantOps(val self: Instant) extends AnyVal {

    def diff(instant: Instant): Long = {
      (self.toEpochMilli - instant.toEpochMilli).abs
    }
  }


  implicit class TimeUnitOps(val self: TimeUnit) extends AnyVal {

    def chronoUnit: ChronoUnit = self match {
      case DAYS         => ChronoUnit.DAYS
      case HOURS        => ChronoUnit.HOURS
      case MINUTES      => ChronoUnit.MINUTES
      case SECONDS      => ChronoUnit.SECONDS
      case MILLISECONDS => ChronoUnit.MILLIS
      case MICROSECONDS => ChronoUnit.MICROS
      case NANOSECONDS  => ChronoUnit.NANOS
    }
  }
}
