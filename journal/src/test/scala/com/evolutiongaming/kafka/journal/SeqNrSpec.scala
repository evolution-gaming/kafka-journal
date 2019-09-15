package com.evolutiongaming.kafka.journal

import cats.implicits._
import org.scalatest.{FunSuite, Matchers}

import com.evolutiongaming.kafka.journal.util.OptionHelper._


class SeqNrSpec extends FunSuite with Matchers {

  test("of") {
    SeqNr.of[Option](0) shouldEqual none
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

  test("min.next") {
    SeqNr.min.next[Option] shouldEqual Some(SeqNr(2))
  }

  test("max.next") {
    SeqNr.max.next[Option] shouldEqual None
  }

  test("min.prev") {
    SeqNr.min.prev[Option] shouldEqual None
  }

  test("max.prev") {
    SeqNr.max.prev[Option] shouldEqual Some(SeqNr(Long.MaxValue - 1))
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
