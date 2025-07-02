package com.evolution.kafka.journal

import com.evolution.kafka.journal.ExpireAfter.implicits.*
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration.*

class ExpireAfterTest extends AnyFunSuite with Matchers {

  test("toString") {
    1.minute.toExpireAfter.toString shouldEqual 1.minute.toString()
  }
}
