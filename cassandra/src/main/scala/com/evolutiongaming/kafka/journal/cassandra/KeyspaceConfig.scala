package com.evolutiongaming.kafka.journal.cassandra

import com.evolutiongaming.scassandra.ReplicationStrategyConfig
import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

final case class KeyspaceConfig(
  name: String                                   = "journal",
  replicationStrategy: ReplicationStrategyConfig = ReplicationStrategyConfig.Default,
  autoCreate: Boolean                            = true,
)

object KeyspaceConfig {

  val default: KeyspaceConfig = KeyspaceConfig()

  implicit val configReaderKeyspace: ConfigReader[KeyspaceConfig] = deriveReader
}
