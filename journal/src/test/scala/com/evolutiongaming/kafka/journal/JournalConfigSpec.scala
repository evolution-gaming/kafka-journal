package com.evolutiongaming.kafka.journal

import cats.syntax.all._
import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import pureconfig.ConfigSource

import scala.concurrent.duration._

class JournalConfigSpec extends AnyFunSuite with Matchers {

  test("apply from empty config") {
    val config = ConfigFactory.empty()
    ConfigSource.fromConfig(config).load[JournalConfig] shouldEqual JournalConfig.default.asRight
  }

  test("apply from config") {
    val config  = ConfigFactory.parseURL(getClass.getResource("journal.conf"))
    val default = JournalConfig.default
    val expected = JournalConfig(
      pollTimeout = 1.millis,
      kafka = default
        .kafka
        .copy(
          producer = default.kafka.producer.copy(common = default.kafka.producer.common.copy(clientId = "clientId".some)),
          consumer = default.kafka.consumer.copy(common = default.kafka.consumer.common.copy(clientId = "clientId".some)),
        ),
      headCache = default.headCache.copy(enabled = false),
    )
    ConfigSource.fromConfig(config).load[JournalConfig] shouldEqual expected.asRight
  }
}
