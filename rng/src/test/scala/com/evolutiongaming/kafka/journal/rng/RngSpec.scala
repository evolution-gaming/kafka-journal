package com.evolutiongaming.kafka.journal.rng

import cats.arrow.FunctionK
import org.scalatest.{FunSuite, Matchers}

class RngSpec extends FunSuite with Matchers {

  private val rng = Rng.State(123456789l).mapK(FunctionK.id)

  test("int") {
    rng.int shouldEqual rng.int
    val (rng1, a0) = rng.int
    a0 shouldEqual 1883
    val (_, a1) = rng1.int
    a1 shouldEqual 1820451251
  }

  test("long") {
    rng.long shouldEqual rng.long
    val (rng1, a0) = rng.long
    a0 shouldEqual 8089243869619l
    val (_, a1) = rng1.long
    a1 shouldEqual 5245808146714613004l
  }

  test("float") {
    rng.float shouldEqual rng.float
    val (rng1, a0) = rng.float
    a0 shouldEqual 4.172325E-7f
    val (_, a1) = rng1.float
    a1 shouldEqual 0.4238568f
  }

  test("double") {
    rng.double shouldEqual rng.double
    val (rng1, random) = rng.double
    random shouldEqual 4.3844963359962463E-7
    val (_, a1) = rng1.double
    a1 shouldEqual 0.2843758208196805
  }
}
