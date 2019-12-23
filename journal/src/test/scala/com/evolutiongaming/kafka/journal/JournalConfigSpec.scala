package com.evolutiongaming.kafka.journal

import cats.implicits._
import com.evolutiongaming.skafka.consumer.{AutoOffsetReset, ConsumerConfig}
import com.evolutiongaming.skafka.producer.{Acks, ProducerConfig}
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
    val config = ConfigFactory.parseURL(getClass.getResource("journal.conf"))
    val expected = JournalConfig(
      pollTimeout = 1.millis,
      producer = ProducerConfig(
        common = JournalConfig.default.producer.common.copy(clientId = "clientId".some),
        acks = Acks.All,
        idempotence = true),
      ConsumerConfig(
        common = JournalConfig.default.consumer.common.copy(clientId = "clientId".some),
        groupId = "journal".some,
        autoOffsetReset = AutoOffsetReset.Earliest,
        autoCommit = false),
      headCache = false)
    ConfigSource.fromConfig(config).load[JournalConfig] shouldEqual expected.asRight
  }
}