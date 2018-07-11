package com.evolutiongaming.kafka.journal

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

  // TODO decide on semantic
  /*test(">=") {
    SeqRange(1) >= SeqRange(1) shouldEqual true

    SeqRange(1) >= SeqRange(2) shouldEqual false
    SeqRange(2) >= SeqRange(1) shouldEqual true

    SeqRange(1, 2) >= SeqRange(1, 2) shouldEqual true

    SeqRange(1, 2) >= SeqRange(2, 3) shouldEqual false
    SeqRange(1, 2) >= SeqRange(3, 4) shouldEqual false

    SeqRange(2, 3) >= SeqRange(1, 2) shouldEqual false
    SeqRange(3, 4) >= SeqRange(1, 2) shouldEqual true
  }*/

  // TODO decide on semantic
  /*test("<=") {
    SeqRange(1) <= SeqRange(1) shouldEqual true

    SeqRange(1) <= SeqRange(2) shouldEqual true
    SeqRange(2) <= SeqRange(1) shouldEqual false

    SeqRange(1, 2) <= SeqRange(1, 2) shouldEqual true

    SeqRange(1, 2) <= SeqRange(2, 3) shouldEqual false
    SeqRange(1, 2) <= SeqRange(3, 4) shouldEqual false

    SeqRange(2, 3) <= SeqRange(1, 2) shouldEqual false
    SeqRange(3, 4) <= SeqRange(1, 2) shouldEqual true
  }*/

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
}
