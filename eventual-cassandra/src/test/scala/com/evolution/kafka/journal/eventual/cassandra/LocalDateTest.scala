package com.evolution.kafka.journal.eventual.cassandra

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.time.{Instant, LocalDate as LocalDateJ, ZoneId}

class LocalDateTest extends AnyFunSuite with Matchers {

  test("Instant to LocalDate") {
    val instant = Instant.parse("2019-10-04T10:10:10.00Z")
    val localDate = LocalDateJ.ofInstant(instant, ZoneId.of("UTC"))
    localDate shouldEqual LocalDateJ.of(2019, 10, 4)
  }
}
