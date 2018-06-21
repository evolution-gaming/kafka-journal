package com.evolutiongaming.cassandra

import com.typesafe.config.ConfigFactory
import org.scalatest.{FunSuite, Matchers}

import scala.concurrent.duration._

class SocketConfigSpec extends FunSuite with Matchers {

  test("apply from empty config") {
    val config = ConfigFactory.empty()
    SocketConfig(config) shouldEqual SocketConfig.Default
  }

  test("apply from config") {
    val config = ConfigFactory.parseURL(getClass.getResource("socket.conf"))
    val expected = SocketConfig(
      connectTimeout = 1.millis,
      readTimeout = 2.seconds,
      keepAlive = Some(false),
      reuseAddress = Some(false),
      soLinger = Some(3),
      tcpNoDelay = Some(false),
      receiveBufferSize = Some(4),
      sendBufferSize = Some(5))
    SocketConfig(config) shouldEqual expected
  }
}
