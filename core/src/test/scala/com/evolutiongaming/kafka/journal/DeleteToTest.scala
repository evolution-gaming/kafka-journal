package com.evolutiongaming.kafka.journal

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class DeleteToTest extends AnyFunSuite with Matchers {

  test("toString") {
    SeqNr.min.toDeleteTo.toString shouldEqual SeqNr.min.toString
  }
}
