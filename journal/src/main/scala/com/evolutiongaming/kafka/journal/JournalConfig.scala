package com.evolutiongaming.kafka.journal

import com.evolutiongaming.config.ConfigHelper._
import com.evolutiongaming.skafka.CommonConfig
import com.evolutiongaming.skafka.consumer.{AutoOffsetReset, ConsumerConfig}
import com.evolutiongaming.skafka.producer.{Acks, ProducerConfig}
import com.typesafe.config.Config

import scala.concurrent.duration._

final case class JournalConfig(
  pollTimeout: FiniteDuration = 10.millis,
  producer: ProducerConfig = ProducerConfig(
    common = CommonConfig(
      clientId = Some("journal"),
      sendBufferBytes = 1000000),
    acks = Acks.All,
    idempotence = true),
  consumer: ConsumerConfig = ConsumerConfig(
    common = CommonConfig(
      clientId = Some("journal"),
      receiveBufferBytes = 100000),
    groupId = Some("journal"),
    autoOffsetReset = AutoOffsetReset.Earliest,
    autoCommit = false),
  headCache: Boolean = true)

object JournalConfig {

  val Default: JournalConfig = JournalConfig()


  def apply(config: Config): JournalConfig = apply(config, Default)

  def apply(config: Config, default: => JournalConfig): JournalConfig = {

    def get[T: FromConf](name: String) = config.getOpt[T](name)

    def kafka(name: String) = {
      for {
        common <- get[Config]("kafka")
      } yield {
        common.getOpt[Config](name).fold(common)(_.withFallback(common))
      }
    }

    JournalConfig(
      pollTimeout = get[FiniteDuration]("poll-timeout") getOrElse default.pollTimeout,
      producer = kafka("producer").fold(default.producer)(ProducerConfig(_, default.producer)),
      consumer = kafka("consumer").fold(default.consumer)(ConsumerConfig(_, default.consumer)),
      headCache = get[Boolean]("head-cache.enabled") getOrElse default.headCache)
  }
}