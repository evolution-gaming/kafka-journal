package com.evolutiongaming.kafka.journal

import org.scalatest.{FunSuite, Matchers}

class SeqNrSpec extends FunSuite with Matchers {

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
    SeqNr.Min.next shouldEqual Some(SeqNr(2))
  }

  test("Max.next") {
    SeqNr.Max.next shouldEqual None
  }

  test("Min.prev") {
    SeqNr.Min.prev shouldEqual None
  }

  test("Max.prev") {
    SeqNr.Max.prev shouldEqual Some(SeqNr(Long.MaxValue - 1))
  }

  test("in") {
    SeqNr.Min in SeqRange(SeqNr.Min, SeqNr.Max) shouldEqual true
  }

  test("to") {
    SeqNr.Min to SeqNr.Max shouldEqual SeqRange(SeqNr.Min, SeqNr.Max)
  }

  test("to Min") {
    SeqNr.Min to SeqNr.Min shouldEqual SeqRange(SeqNr.Min)
  }
}
