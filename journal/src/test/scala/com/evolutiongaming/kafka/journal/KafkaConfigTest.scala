package com.evolutiongaming.kafka.journal

import cats.syntax.all.*
import com.evolutiongaming.skafka.CommonConfig
import com.evolutiongaming.skafka.consumer.{AutoOffsetReset, ConsumerConfig}
import com.evolutiongaming.skafka.producer.{Acks, CompressionType, ProducerConfig}
import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import pureconfig.{ConfigReader, ConfigSource}

import scala.concurrent.duration.*

class KafkaConfigTest extends AnyFunSuite with Matchers {

  private implicit val configReader: ConfigReader[KafkaConfig] = KafkaConfig.configReader(KafkaConfig.default)

  test("configReader from empty config") {
    val config = ConfigFactory.empty()
    ConfigSource.fromConfig(config).load[KafkaConfig] shouldEqual KafkaConfig.default.asRight
  }

  test("configReader from config") {
    val config = ConfigFactory.parseURL(getClass.getResource("kafka.conf"))
    val expected = KafkaConfig(
      name = "test",
      producer = ProducerConfig(
        common          = CommonConfig(clientId = "clientId".some, sendBufferBytes = 1000, receiveBufferBytes = 100),
        acks            = Acks.All,
        idempotence     = true,
        linger          = 1.millis,
        compressionType = CompressionType.Lz4,
      ),
      ConsumerConfig(
        common          = CommonConfig(clientId = "clientId".some, sendBufferBytes = 100, receiveBufferBytes = 1000),
        groupId         = "groupId".some,
        autoOffsetReset = AutoOffsetReset.Earliest,
        autoCommit      = false,
        maxPollRecords  = 1000,
      ),
    )
    ConfigSource.fromConfig(config).load[KafkaConfig] shouldEqual expected.asRight
  }
}
