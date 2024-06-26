package com.evolutiongaming.kafka.journal.replicator

import cats.implicits._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.time.{LocalDateTime, LocalTime}
import scala.concurrent.duration._
import scala.util.Try

class DurationToTest extends AnyFunSuite with Matchers {

  private val now = LocalDateTime.now()

  for {
    now   <- List(now, now.plusHours(1), now.minusHours(1), now.plusDays(1), now.minusDays(1))
    until <- List(LocalTime.of(3, 0), LocalTime.of(9, 0), LocalTime.of(15, 0), LocalTime.of(21, 0))
  }
    test(s"valid duration from $now until $until") {
      val duration = DurationTo[Try](now.pure[Try])
        .apply(until)
        .get
      duration should be >= 0.hours
      duration should be <= 24.hours
    }
}
