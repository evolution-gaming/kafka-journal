package com.evolutiongaming.cassandra

import com.typesafe.config.ConfigFactory
import org.scalatest.{FunSuite, Matchers}

import scala.concurrent.duration._

class PoolingConfigSpec extends FunSuite with Matchers {

  test("apply from empty config") {
    val config = ConfigFactory.empty()
    PoolingConfig(config) shouldEqual PoolingConfig.Default
  }

  test("apply from config") {
    val config = ConfigFactory.parseURL(getClass.getResource("pooling.conf"))
    val expected = PoolingConfig(
      local = PoolingConfig.HostConfig(
        newConnectionThreshold = 1,
        maxRequestsPerConnection = 2,
        connectionsPerHostCore = 3,
        connectionsPerHostMax = 4),
      remote = PoolingConfig.HostConfig(
        newConnectionThreshold = 5,
        maxRequestsPerConnection = 6,
        connectionsPerHostCore = 7,
        connectionsPerHostMax = 8),
      poolTimeout = 1.millis,
      idleTimeout = 2.seconds,
      maxQueueSize = 3,
      heartbeatInterval = 4.hours)
    PoolingConfig(config) shouldEqual expected
  }
}