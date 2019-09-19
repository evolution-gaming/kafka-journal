package com.evolutiongaming.kafka.journal

import cats.implicits._
import com.evolutiongaming.kafka.journal.util.OptionHelper._
import org.scalatest.{FunSuite, Matchers}


class SeqNrSpec extends FunSuite with Matchers {

  test("of") {
    SeqNr.of[Option](0) shouldEqual none
  }

  test("show") {
    SeqNr.min.show shouldEqual "1"
    SeqNr.max.show shouldEqual "9223372036854775807"
  }

  test("1 max 2") {
    SeqNr.unsafe(1) max SeqNr.unsafe(2) shouldEqual SeqNr.unsafe(2)
  }

  test("2 max 1") {
    SeqNr.unsafe(2) max SeqNr.unsafe(1) shouldEqual SeqNr.unsafe(2)
  }

  test("1 min 2") {
    SeqNr.unsafe(1) min SeqNr.unsafe(2) shouldEqual SeqNr.unsafe(1)
  }

  test("2 min 1") {
    SeqNr.unsafe(2) min SeqNr.unsafe(1) shouldEqual SeqNr.unsafe(1)
  }

  test("min.next") {
    SeqNr.min.next[Option] shouldEqual Some(SeqNr.unsafe(2))
  }

  test("max.next") {
    SeqNr.max.next[Option] shouldEqual None
  }

  test("min.prev") {
    SeqNr.min.prev[Option] shouldEqual None
  }

  test("max.prev") {
    SeqNr.max.prev[Option] shouldEqual Some(SeqNr.unsafe(Long.MaxValue - 1))
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
