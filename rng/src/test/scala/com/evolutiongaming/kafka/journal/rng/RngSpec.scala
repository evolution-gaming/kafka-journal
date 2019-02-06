package com.evolutiongaming.kafka.journal.rng

import org.scalatest.{FunSuite, Matchers}

class RngSpec extends FunSuite with Matchers {

  test("int") {
    val rng = Rng(123456789l)
    rng.int shouldEqual rng.int
    val (a0, rng1) = rng.int
    a0 shouldEqual 1883
    val (a1, _) = rng1.int
    a1 shouldEqual 1820451251
  }

  test("double") {
    val rng = Rng(123456789l)
    rng.double shouldEqual rng.double
    val (random, rng1) = rng.double
    random shouldEqual 4.3844963359962463E-7
    val (a1, _) = rng1.double
    a1 shouldEqual 0.2843758208196805
  }
}
