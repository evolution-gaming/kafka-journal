package com.evolution.kafka.journal

import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

import scala.concurrent.duration.*

/**
 * Kafka-specific configuration used by a plugin.
 *
 * The main reason to keep the configuration separate is to have the ability to avoid defining
 * Cassandra-specific configuration for the modules not requiring to access Cassandra such as
 * [[AppendReplicateApp]].
 *
 * @param pollTimeout
 *   The timeout used for `poll` requests in Kafka client. The usual considerations apply, i.e.
 *   smaller timeout means better latency and larger timeout may mean a better throughput.
 * @param kafka
 *   Kafka client configuration, see [[KafkaConfig]] for more details.
 * @param headCache
 *   Configuration of head cache, i.e. whether it is enabled and how it is tuned. See [[HeadCache]]
 *   for more details on what is head cache and how it works.
 */
final case class JournalConfig(
  pollTimeout: FiniteDuration = 10.millis,
  kafka: KafkaConfig = KafkaConfig("journal"),
  headCache: JournalConfig.HeadCache = JournalConfig.HeadCache.default,
)

object JournalConfig {

  val default: JournalConfig = JournalConfig()

  implicit val configReaderJournalConfig: ConfigReader[JournalConfig] = {
    implicit val configReaderKafkaConfig: ConfigReader[KafkaConfig] = KafkaConfig.configReader(default.kafka)
    deriveReader[JournalConfig]
  }

  /**
   * Configuration of head cache.
   *
   * The tuning parameters of [[HeadCacheConfig]] are read from the same config object as `enabled`,
   * i.e. `head-cache.partition.max-size` and not `head-cache.config.partition.max-size`.
   *
   * @param enabled
   *   The head cache will be used on journal recovery if `true`. Every recovery may need to read
   *   both Cassandra (or other long term storage) and Kafka otherwise.
   * @param config
   *   Tuning parameters of the head cache, see [[HeadCacheConfig]].
   *
   * @see
   *   [[com.evolution.kafka.journal.HeadCache]] for more details on what is head cache and how it
   *   works.
   */
  final case class HeadCache(enabled: Boolean = true, config: HeadCacheConfig = HeadCacheConfig.default)

  object HeadCache {

    val default: HeadCache = HeadCache()

    private final case class Enabled(enabled: Boolean = default.enabled)

    implicit val configReaderHeadCache: ConfigReader[HeadCache] = {
      val configReaderEnabled: ConfigReader[Enabled] = deriveReader
      configReaderEnabled.zip(HeadCacheConfig.configReaderHeadCacheConfig).map {
        case (Enabled(enabled), config) => HeadCache(enabled = enabled, config = config)
      }
    }
  }
}
