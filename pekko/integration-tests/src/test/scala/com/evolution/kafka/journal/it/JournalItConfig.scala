package com.evolution.kafka.journal.it

import com.evolution.kafka.journal.JournalConfig
import com.evolution.kafka.journal.eventual.cassandra.EventualCassandraConfig
import com.evolution.kafka.journal.pekko.persistence.KafkaJournalConfig
import com.evolution.kafka.journal.replicator.ReplicatorConfig
import com.typesafe.config.Config
import pureconfig.ConfigSource

import scala.util.Try

/**
 * Kafka-Journal integration test config.
 *
 * Obtain one using [[SharedItEnv]].
 *
 * @param unparsed
 *   full unparsed config object, useful for Pekko-based integration tests
 * @param eventualCassandra
 *   parsed [[EventualCassandraConfig]]
 * @param journal
 *   parsed [[JournalConfig]]
 * @param replicator
 *   parsed [[ReplicatorConfig]]
 */
private[it] final case class JournalItConfig(
  unparsed: Config,
  eventualCassandra: EventualCassandraConfig,
  journal: JournalConfig,
  replicator: ReplicatorConfig,
)

private[it] object JournalItConfig {

  /**
   * Parses [[JournalItConfig]] - expects [[KafkaJournalConfig]] and [[ReplicatorConfig]] on the
   * default paths.
   *
   * Throws exceptions.
   */
  def parseUnsafe(config: Config): JournalItConfig = {
    val kafkaJournalPluginConfig = ConfigSource.fromConfig(config)
      .at("evolutiongaming.kafka-journal.persistence.journal")
      .loadOrThrow[KafkaJournalConfig]
    JournalItConfig(
      unparsed = config,
      eventualCassandra = kafkaJournalPluginConfig.cassandra,
      journal = kafkaJournalPluginConfig.journal,
      replicator = ReplicatorConfig.fromConfig[Try](config).get,
    )
  }
}
