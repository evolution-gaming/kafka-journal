package com.evolutiongaming.kafka.journal

import org.scalatest.FunSuite
import org.scalatest.matchers.should.Matchers

class DeleteToTest extends FunSuite with Matchers {

  test("toString") {
    SeqNr.min.toDeleteTo.toString shouldEqual SeqNr.min.toString
  }
}

