package com.evolutiongaming.cassandra

import com.typesafe.config.ConfigFactory
import org.scalatest.{FunSuite, Matchers}

import scala.concurrent.duration._

class ReconnectionConfigSpec extends FunSuite with Matchers {

  test("apply from empty config") {
    val config = ConfigFactory.empty()
    ReconnectionConfig(config) shouldEqual ReconnectionConfig.Default
  }

  test("apply from config") {
    val config = ConfigFactory.parseURL(getClass.getResource("reconnection.conf"))
    val expected = ReconnectionConfig(
      minDelay = 1.millis,
      maxDelay = 2.seconds)
    ReconnectionConfig(config) shouldEqual expected
  }
}
