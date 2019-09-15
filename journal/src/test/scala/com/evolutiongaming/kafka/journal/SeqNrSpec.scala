package com.evolutiongaming.kafka.journal

import cats.implicits._
import org.scalatest.{FunSuite, Matchers}

class SeqNrSpec extends FunSuite with Matchers {

  test("of") {
    SeqNr.of[Either[String, ?]](0).isRight shouldEqual false
  }

  test("1 max 2") {
    SeqNr(1) max SeqNr(2) shouldEqual SeqNr(2)
  }

  test("2 max 1") {
    SeqNr(2) max SeqNr(1) shouldEqual SeqNr(2)
  }

  test("1 min 2") {
    SeqNr(1) min SeqNr(2) shouldEqual SeqNr(1)
  }

  test("2 min 1") {
    SeqNr(2) min SeqNr(1) shouldEqual SeqNr(1)
  }

  test("Min.next") {
    SeqNr.min.next shouldEqual Some(SeqNr(2))
  }

  test("Max.next") {
    SeqNr.max.next shouldEqual None
  }

  test("Min.prev") {
    SeqNr.min.prev shouldEqual None
  }

  test("Max.prev") {
    SeqNr.max.prev shouldEqual Some(SeqNr(Long.MaxValue - 1))
  }

  test("in") {
    SeqNr.min in SeqRange(SeqNr.min, SeqNr.max) shouldEqual true
  }

  test("to") {
    SeqNr.min to SeqNr.max shouldEqual SeqRange(SeqNr.min, SeqNr.max)
  }

  test("to Min") {
    SeqNr.min to SeqNr.min shouldEqual SeqRange(SeqNr.min)
  }
}
