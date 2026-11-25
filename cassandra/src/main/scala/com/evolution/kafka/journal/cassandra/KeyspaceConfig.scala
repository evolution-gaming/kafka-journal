package com.evolution.kafka.journal.cassandra

import com.evolution.scassandra.ReplicationStrategyConfig
import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

private[journal] final case class KeyspaceConfig(
  name: String = "journal",
  replicationStrategy: ReplicationStrategyConfig = ReplicationStrategyConfig.Default,
  autoCreate: Boolean = true,
)

private[journal] object KeyspaceConfig {

  val default: KeyspaceConfig = KeyspaceConfig()

  implicit val configReaderKeyspace: ConfigReader[KeyspaceConfig] = deriveReader
}
