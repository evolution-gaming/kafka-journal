package com.evolutiongaming.kafka.journal

import org.scalatest.matchers.should.Matchers
import org.scalatest.funsuite.AnyFunSuite

class DeleteToTest extends AnyFunSuite with Matchers {

  test("toString") {
    SeqNr.min.toDeleteTo.toString shouldEqual SeqNr.min.toString
  }
}

