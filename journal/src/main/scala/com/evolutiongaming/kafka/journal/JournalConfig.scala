package com.evolutiongaming.kafka.journal

import cats.implicits._
import com.evolutiongaming.config.ConfigHelper._
import com.evolutiongaming.skafka.CommonConfig
import com.evolutiongaming.skafka.consumer.{AutoOffsetReset, ConsumerConfig}
import com.evolutiongaming.skafka.producer.{Acks, ProducerConfig}
import com.typesafe.config.Config
import pureconfig.error.{ConfigReaderFailures, ThrowableFailure}
import pureconfig.{ConfigCursor, ConfigReader}

import scala.concurrent.duration._
import scala.util.Try


final case class JournalConfig(
  pollTimeout: FiniteDuration = 10.millis,
  producer: ProducerConfig = ProducerConfig(
    common = CommonConfig(
      clientId = "journal".some,
      sendBufferBytes = 1000000),
    acks = Acks.All,
    idempotence = true),
  consumer: ConsumerConfig = ConsumerConfig(
    common = CommonConfig(
      clientId = "journal".some,
      receiveBufferBytes = 100000),
    groupId = "journal".some,
    autoOffsetReset = AutoOffsetReset.Earliest,
    autoCommit = false),
  headCache: Boolean = true)

object JournalConfig {

  val default: JournalConfig = JournalConfig()

  implicit val configReaderJournalConfig: ConfigReader[JournalConfig] = {
    cursor: ConfigCursor => {
      for {
        cursor  <- cursor.asObjectCursor
        journal  = Try { fromConfig(cursor.value.toConfig, default) }
        journal <- journal.toEither.leftMap(a => ConfigReaderFailures(ThrowableFailure(a, cursor.location)))
      } yield journal
    }
  }


  @deprecated("use ConfigReader instead", "0.0.87")
  def apply(config: Config): JournalConfig = apply(config, default)

  @deprecated("use ConfigReader instead", "0.0.87")
  def apply(config: Config, default: => JournalConfig): JournalConfig = fromConfig(config, default)

  
  def fromConfig(config: Config, default: => JournalConfig): JournalConfig = {

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