package com.evolutiongaming.kafka.journal

import cats.implicits._
import com.evolutiongaming.skafka.CommonConfig
import com.evolutiongaming.skafka.consumer.{AutoOffsetReset, ConsumerConfig}
import com.evolutiongaming.skafka.producer.{Acks, ProducerConfig}
import com.typesafe.config.Config
import pureconfig.error.{ConfigReaderFailures, ThrowableFailure}
import pureconfig.{ConfigCursor, ConfigReader, ConfigSource, Derivation}

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


  def fromConfig(config: Config, default: => JournalConfig): JournalConfig = {

    val source = ConfigSource.fromConfig(config)

    def get[A](name: String)(implicit reader: Derivation[ConfigReader[A]]) = {
      source.at(name).load[A]
    }

    def kafka(name: String) = {
      get[Config]("kafka").map { kafka =>
        ConfigSource.fromConfig(kafka)
          .at(name)
          .load[Config]
          .fold(_ => kafka, _.withFallback(kafka))
      }
    }

    JournalConfig(
      pollTimeout = get[FiniteDuration]("poll-timeout") getOrElse default.pollTimeout,
      producer = kafka("producer").fold(_ => default.producer, ProducerConfig(_, default.producer)),
      consumer = kafka("consumer").fold(_ => default.consumer, ConsumerConfig(_, default.consumer)),
      headCache = get[Boolean]("head-cache.enabled") getOrElse default.headCache)
  }
}