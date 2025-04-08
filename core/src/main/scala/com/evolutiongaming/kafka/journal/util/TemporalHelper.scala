package com.evolutiongaming.kafka.journal.util

import cats.Order

import java.time.temporal.{ChronoUnit, Temporal, TemporalAmount}
import java.time.{Instant, LocalDate, ZoneId}
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.*

private[journal] object TemporalHelper {

  implicit val instantOrdering: Ordering[Instant] = Ordering.fromLessThan(_ isBefore _)

  implicit val instantOrder: Order[Instant] = Order.fromOrdering

  implicit val localDateOrdering: Ordering[LocalDate] = Ordering.fromLessThan(_ isBefore _)

  implicit val localDateOrder: Order[LocalDate] = Order.fromOrdering

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

    def diff(instant: Instant): FiniteDuration = {
      (self.toEpochMilli - instant.toEpochMilli).abs.millis
    }

    def toLocalDate(zoneId: ZoneId): LocalDate = LocalDate.ofInstant(self, zoneId)
  }

  implicit class TimeUnitOps(val self: TimeUnit) extends AnyVal {

    def chronoUnit: ChronoUnit = self match {
      case DAYS => ChronoUnit.DAYS
      case HOURS => ChronoUnit.HOURS
      case MINUTES => ChronoUnit.MINUTES
      case SECONDS => ChronoUnit.SECONDS
      case MILLISECONDS => ChronoUnit.MILLIS
      case MICROSECONDS => ChronoUnit.MICROS
      case NANOSECONDS => ChronoUnit.NANOS
    }
  }
}
