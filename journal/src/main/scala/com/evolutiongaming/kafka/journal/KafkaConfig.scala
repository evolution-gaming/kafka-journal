package com.evolutiongaming.kafka.journal

import cats.syntax.all._
import com.evolutiongaming.skafka.CommonConfig
import com.evolutiongaming.skafka.consumer.{AutoOffsetReset, ConsumerConfig}
import com.evolutiongaming.skafka.producer.{Acks, CompressionType, ProducerConfig}
import com.typesafe.config.Config
import pureconfig.{ConfigCursor, ConfigReader, ConfigSource}

import scala.concurrent.duration._

/** Configuration of Kafka client used to store the journal.
  *
  * The configuration uses standard consumer and producer configuration from
  * skafka. It is highly recommended to keep the default settings unless some
  * internal issue is to be worked around as the logic of persistence plugin may
  * depend on some of these settings.
  *
  * @param producer
  *   Producer configuration used to write journal to Kafka.
  * @param consumer
  *   Consumer configuration used to read journal on recovery.
  */
final case class KafkaConfig(producer: ProducerConfig, consumer: ConsumerConfig)

object KafkaConfig {

  val default: KafkaConfig = apply("kafka-journal")

  def apply(name: String): KafkaConfig = {
    val common = CommonConfig(clientId = name.some, sendBufferBytes = 1000000, receiveBufferBytes = 1000000)
    KafkaConfig(
      producer = ProducerConfig(
        common          = common,
        acks            = Acks.All,
        idempotence     = true,
        linger          = 1.millis,
        compressionType = CompressionType.Lz4,
      ),
      consumer = ConsumerConfig(
        common          = common,
        groupId         = name.some,
        autoOffsetReset = AutoOffsetReset.Earliest,
        autoCommit      = false,
        maxPollRecords  = 1000,
      ),
    )
  }

  def configReader(default: => KafkaConfig): ConfigReader[KafkaConfig] = { (cursor: ConfigCursor) =>
    {
      for {
        cursor <- cursor.asObjectCursor
      } yield {
        val config = cursor.objValue.toConfig
        val source = ConfigSource.fromConfig(config)

        def at(name: String) = {
          source
            .at(name)
            .load[Config]
            .fold(_ => config, _.withFallback(config))
        }

        KafkaConfig(
          producer = ProducerConfig(at("producer"), default.producer),
          consumer = ConsumerConfig(at("consumer"), default.consumer),
        )
      }
    }
  }
}
