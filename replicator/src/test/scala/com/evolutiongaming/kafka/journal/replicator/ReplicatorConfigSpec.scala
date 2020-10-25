package com.evolutiongaming.kafka.journal.replicator

import cats.data.{NonEmptyList => Nel}
import cats.syntax.all._
import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import pureconfig.{ConfigReader, ConfigSource}

import scala.concurrent.duration._


class ReplicatorConfigSpec extends AnyFunSuite with Matchers {

  test("apply from empty config") {
    val config = ConfigFactory.empty()
    val expected = ReplicatorConfig.default
    ConfigSource.fromConfig(config).load[ReplicatorConfig] shouldEqual expected.pure[ConfigReader.Result]
  }

  test("apply from config") {
    val config = ConfigFactory.parseURL(getClass.getResource("replicator.conf"))
    val expected = ReplicatorConfig(
      topicPrefixes = Nel.of("prefix1", "prefix2"),
      topicDiscoveryInterval = 1.minute,
      pollTimeout = 200.millis)
    ConfigSource.fromConfig(config).load[ReplicatorConfig] shouldEqual expected.pure[ConfigReader.Result]
  }

  test("apply from config with common kafka") {
    val config = ConfigFactory.parseURL(getClass.getResource("replicator-kafka.conf"))
    val default = ReplicatorConfig.default
    val expected = ReplicatorConfig(
      topicPrefixes = Nel.of("prefix"),
      kafka = default.kafka.copy(
        producer = default.kafka.producer.copy(
          common = default.kafka.producer.common.copy(
            clientId = "clientId".some)),
        consumer = default.kafka.consumer.copy(
          maxPollRecords = 10,
          common = default.kafka.consumer.common.copy(
            clientId = "clientId".some))))
    ConfigSource.fromConfig(config).load[ReplicatorConfig] shouldEqual expected.pure[ConfigReader.Result]
  }

  test("apply from reference.conf") {
    val config = ConfigFactory.load()
    val expected = ReplicatorConfig.default
    ConfigSource
      .fromConfig(config)
      .at("evolutiongaming.kafka-journal.replicator")
      .load[ReplicatorConfig] shouldEqual expected.pure[ConfigReader.Result]
  }
}