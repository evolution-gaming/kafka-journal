package com.evolutiongaming.kafka.journal

import com.evolutiongaming.nel.Nel
import org.scalatest.{FunSuite, Matchers}

class SeqRangeSpec extends FunSuite with Matchers {

  test("==") {
    SeqRange(1) == SeqRange(1) shouldEqual true
    SeqRange(1) == SeqRange(2) shouldEqual false
    SeqRange(2) == SeqRange(1) shouldEqual false
    SeqRange(1, 2) == SeqRange(1, 2) shouldEqual true
    SeqRange(1, 2) == SeqRange(2, 3) shouldEqual false
    SeqRange(2, 3) == SeqRange(1, 2) shouldEqual false
  }

  test(">") {
    SeqRange(1) > SeqRange(1) shouldEqual false
    SeqRange(1) > SeqRange(2) shouldEqual false
    SeqRange(2) > SeqRange(1) shouldEqual true

    SeqRange(1, 2) > SeqRange(1, 2) shouldEqual false
    SeqRange(1, 2) > SeqRange(2, 3) shouldEqual false
    SeqRange(1, 2) > SeqRange(3, 4) shouldEqual false

    SeqRange(2, 3) > SeqRange(1, 2) shouldEqual false
    SeqRange(3, 4) > SeqRange(1, 2) shouldEqual true
  }

  test("<") {
    SeqRange(1) < SeqRange(1) shouldEqual false

    SeqRange(1) < SeqRange(2) shouldEqual true
    SeqRange(2) < SeqRange(1) shouldEqual false

    SeqRange(1, 2) < SeqRange(1, 2) shouldEqual false

    SeqRange(1, 2) < SeqRange(2, 3) shouldEqual false
    SeqRange(1, 2) < SeqRange(3, 4) shouldEqual true

    SeqRange(2, 3) < SeqRange(1, 2) shouldEqual false
    SeqRange(3, 4) < SeqRange(1, 2) shouldEqual false
  }

  test("toNel") {
    SeqRange(1).toNel.map(_.value) shouldEqual Nel(1)
    SeqRange(1, 2).toNel.map(_.value) shouldEqual Nel(1, 2)
    SeqRange(1, 4).toNel.map(_.value) shouldEqual Nel(1, 2, 3, 4)
    SeqRange(SeqNr.Min).toNel shouldEqual Nel(SeqNr.Min)
    SeqRange(SeqNr.Max).toNel shouldEqual Nel(SeqNr.Max)
  }

  test("contains") {
    SeqRange(1) contains SeqRange(1) shouldEqual true

    SeqRange(1) contains SeqRange(2) shouldEqual false
    SeqRange(2) contains SeqRange(1) shouldEqual false

    SeqRange(1, 2) contains SeqRange(1, 2) shouldEqual true

    SeqRange(1, 2) contains SeqRange(2, 3) shouldEqual false
    SeqRange(1, 2) contains SeqRange(3, 4) shouldEqual false

    SeqRange(2, 3) contains SeqRange(1, 2) shouldEqual false
    SeqRange(3, 4) contains SeqRange(1, 2) shouldEqual false

    SeqRange(1, 4) contains SeqRange(2, 3) shouldEqual true
  }

  test("intersects") {
    SeqRange(1) intersects SeqRange(1) shouldEqual true

    SeqRange(1) intersects SeqRange(2) shouldEqual false
    SeqRange(2) intersects SeqRange(1) shouldEqual false

    SeqRange(1, 2) intersects SeqRange(1, 2) shouldEqual true

    SeqRange(1, 2) intersects SeqRange(2, 3) shouldEqual true
    SeqRange(1, 2) intersects SeqRange(3, 4) shouldEqual false

    SeqRange(2, 3) intersects SeqRange(1, 2) shouldEqual true
    SeqRange(3, 4) intersects SeqRange(1, 2) shouldEqual false

    SeqRange(1, 4) intersects SeqRange(2, 3) shouldEqual true
    SeqRange(2, 3) intersects SeqRange(1, 4) shouldEqual true
  }
}
