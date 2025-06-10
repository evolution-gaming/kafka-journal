package com.evolutiongaming.kafka.journal

import com.evolutiongaming.kafka.journal.ExpireAfter.implicits.*
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration.*

class ExpireAfterTest extends AnyFunSuite with Matchers {

  test("toString") {
    1.minute.toExpireAfter.toString shouldEqual 1.minute.toString()
  }
}
