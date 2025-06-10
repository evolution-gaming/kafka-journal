package com.evolutiongaming.kafka.journal

import cats.data.NonEmptyList as Nel
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class SeqRangeSpec extends AnyFunSuite with Matchers {

  private def seqRange(value: Int) = SeqRange(SeqNr.unsafe(value))

  private def seqRange(from: Int, to: Int) = SeqRange(SeqNr.unsafe(from), SeqNr.unsafe(to))

  test("==") {
    seqRange(1) == seqRange(1) shouldEqual true
    seqRange(1) == seqRange(2) shouldEqual false
    seqRange(2) == seqRange(1) shouldEqual false
    seqRange(1, 2) == seqRange(1, 2) shouldEqual true
    seqRange(1, 2) == seqRange(2, 3) shouldEqual false
    seqRange(2, 3) == seqRange(1, 2) shouldEqual false
  }

  test(">") {
    seqRange(1) > seqRange(1) shouldEqual false
    seqRange(1) > seqRange(2) shouldEqual false
    seqRange(2) > seqRange(1) shouldEqual true

    seqRange(1, 2) > seqRange(1, 2) shouldEqual false
    seqRange(1, 2) > seqRange(2, 3) shouldEqual false
    seqRange(1, 2) > seqRange(3, 4) shouldEqual false

    seqRange(2, 3) > seqRange(1, 2) shouldEqual false
    seqRange(3, 4) > seqRange(1, 2) shouldEqual true
  }

  test("<") {
    seqRange(1) < seqRange(1) shouldEqual false

    seqRange(1) < seqRange(2) shouldEqual true
    seqRange(2) < seqRange(1) shouldEqual false

    seqRange(1, 2) < seqRange(1, 2) shouldEqual false

    seqRange(1, 2) < seqRange(2, 3) shouldEqual false
    seqRange(1, 2) < seqRange(3, 4) shouldEqual true

    seqRange(2, 3) < seqRange(1, 2) shouldEqual false
    seqRange(3, 4) < seqRange(1, 2) shouldEqual false
  }

  test("toNel") {
    seqRange(1).toNel.map(_.value) shouldEqual Nel.of(1)
    seqRange(1, 2).toNel.map(_.value) shouldEqual Nel.of(1, 2)
    seqRange(1, 4).toNel.map(_.value) shouldEqual Nel.of(1, 2, 3, 4)
    SeqRange(SeqNr.min).toNel shouldEqual Nel.of(SeqNr.min)
    SeqRange(SeqNr.max).toNel shouldEqual Nel.of(SeqNr.max)
  }

  test("contains") {
    seqRange(1) contains seqRange(1) shouldEqual true

    seqRange(1) contains seqRange(2) shouldEqual false
    seqRange(2) contains seqRange(1) shouldEqual false

    seqRange(1, 2) contains seqRange(1, 2) shouldEqual true

    seqRange(1, 2) contains seqRange(2, 3) shouldEqual false
    seqRange(1, 2) contains seqRange(3, 4) shouldEqual false

    seqRange(2, 3) contains seqRange(1, 2) shouldEqual false
    seqRange(3, 4) contains seqRange(1, 2) shouldEqual false

    seqRange(1, 4) contains seqRange(2, 3) shouldEqual true
  }

  test("intersects") {
    seqRange(1) intersects seqRange(1) shouldEqual true

    seqRange(1) intersects seqRange(2) shouldEqual false
    seqRange(2) intersects seqRange(1) shouldEqual false

    seqRange(1, 2) intersects seqRange(1, 2) shouldEqual true

    seqRange(1, 2) intersects seqRange(2, 3) shouldEqual true
    seqRange(1, 2) intersects seqRange(3, 4) shouldEqual false

    seqRange(2, 3) intersects seqRange(1, 2) shouldEqual true
    seqRange(3, 4) intersects seqRange(1, 2) shouldEqual false

    seqRange(1, 4) intersects seqRange(2, 3) shouldEqual true
    seqRange(2, 3) intersects seqRange(1, 4) shouldEqual true
  }
}
