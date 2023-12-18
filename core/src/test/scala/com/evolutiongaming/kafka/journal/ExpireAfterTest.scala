package com.evolutiongaming.kafka.journal

import com.evolutiongaming.kafka.journal.ExpireAfter.implicits._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration._

class ExpireAfterTest extends AnyFunSuite with Matchers {

  test("toString") {
    1.minute.toExpireAfter.toString shouldEqual 1.minute.toString()
  }
}
