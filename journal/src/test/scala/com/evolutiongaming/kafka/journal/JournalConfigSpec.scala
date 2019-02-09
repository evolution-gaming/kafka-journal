package com.evolutiongaming.kafka.journal

import com.evolutiongaming.skafka.consumer.{AutoOffsetReset, ConsumerConfig}
import com.evolutiongaming.skafka.producer.{Acks, ProducerConfig}
import com.typesafe.config.ConfigFactory
import org.scalatest.{FunSuite, Matchers}

import scala.concurrent.duration._


class JournalConfigSpec extends FunSuite with Matchers {

  test("apply from empty config") {
    val config = ConfigFactory.empty()
    JournalConfig(config) shouldEqual JournalConfig.Default
  }

  test("apply from config") {
    val config = ConfigFactory.parseURL(getClass.getResource("journal.conf"))
    JournalConfig(config) shouldEqual JournalConfig(
      pollTimeout = 1.millis,
      producer = ProducerConfig(
        common = JournalConfig.Default.producer.common.copy(clientId = Some("clientId")),
        acks = Acks.All,
        idempotence = true),
      ConsumerConfig(
        common = JournalConfig.Default.consumer.common.copy(clientId = Some("clientId")),
        groupId = Some("journal"),
        autoOffsetReset = AutoOffsetReset.Earliest,
        maxPollRecords = 100,
        autoCommit = false),
      headCache = false)
  }
}