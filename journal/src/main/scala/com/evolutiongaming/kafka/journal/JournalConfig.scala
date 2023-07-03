package com.evolutiongaming.kafka.journal

import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

import scala.concurrent.duration._

/** Kafka-specific configuration used by a plugin.
  *
  * The main reason to keep the configuration separate is to have the ability to
  * avoid defining Cassandra-specific configuration for the modules not
  * requiring to access Cassandra such as [[AppendReplicateApp]].
  *
  * @param pollTimeout
  *   The timeout used for `poll` requests in Kafka client. The usual
  *   considerations apply, i.e. smaller timeout means better latency and larger
  *   timeout may mean a better throughput.
  * @param kafka
  *   Kafka client configuration, see [[KafkaConfig]] for more details.
  * @param headCache
  *   Configuration of head cache, which is, currently, only about enabling or
  *   disabling it. See [[HeadCache]] for more details on what is head cache
  *   and how it works.
  */
final case class JournalConfig(
  pollTimeout: FiniteDuration = 10.millis,
  kafka: KafkaConfig = KafkaConfig("journal"),
  headCache: JournalConfig.HeadCache = JournalConfig.HeadCache.default)

object JournalConfig {

  val default: JournalConfig = JournalConfig()

  implicit val configReaderJournalConfig: ConfigReader[JournalConfig] = {
    implicit val configReaderKafkaConfig = KafkaConfig.configReader(default.kafka)
    deriveReader[JournalConfig]
  }


  final case class HeadCache(enabled: Boolean = true)

  object HeadCache {

    val default: HeadCache = HeadCache()

    implicit val configReaderHeadCache: ConfigReader[HeadCache] = deriveReader
  }
}
