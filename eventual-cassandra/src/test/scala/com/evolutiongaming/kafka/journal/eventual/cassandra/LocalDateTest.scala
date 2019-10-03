package com.evolutiongaming.kafka.journal.eventual.cassandra

import java.time.{Instant, ZoneId, LocalDate => LocalDateJ}

import com.datastax.driver.core.LocalDate
import org.scalatest.{FunSuite, Matchers}

class LocalDateTest extends FunSuite with Matchers {

  test("datastax LocalDate to/from java LocalDate") {
    val localDateJ = LocalDateJ.of(2019, 10, 4)
    val localDate = LocalDate.fromYearMonthDay(2019, 10, 4)
    LocalDateJ.ofEpochDay(localDate.getDaysSinceEpoch.toLong) shouldEqual localDateJ
    LocalDate.fromDaysSinceEpoch(localDateJ.toEpochDay.toInt) shouldEqual localDate
  }

  test("Instant to LocalDate") {
    val instant = Instant.parse("2019-10-04T10:10:10.00Z")
    val localDate = LocalDateJ.ofInstant(instant, ZoneId.of("UTC"))
    localDate shouldEqual LocalDateJ.of(2019, 10, 4)
  }
}
