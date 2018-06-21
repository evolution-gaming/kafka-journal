package com.evolutiongaming.cassandra

import com.typesafe.config.ConfigFactory
import org.scalatest.{FunSuite, Matchers}

import scala.concurrent.duration._

class SpeculativeExecutionConfigSpec extends FunSuite with Matchers {

  test("apply from empty config") {
    val config = ConfigFactory.empty()
    SpeculativeExecutionConfig(config) shouldEqual SpeculativeExecutionConfig.Default
  }

  test("apply from config") {
    val config = ConfigFactory.parseURL(getClass.getResource("speculative-execution.conf"))
    val expected = SpeculativeExecutionConfig(
      delay = 1.millis,
      maxExecutions = 3)
    SpeculativeExecutionConfig(config) shouldEqual expected
  }
}