package com.evolutiongaming.kafka.journal.cassandra

import com.evolutiongaming.scassandra.ReplicationStrategyConfig
import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

private[journal] final case class KeyspaceConfig(
    name: String                                   = "journal",
    replicationStrategy: ReplicationStrategyConfig = ReplicationStrategyConfig.Default,
    autoCreate: Boolean                            = true,
)

private[journal] object KeyspaceConfig {

  val default: KeyspaceConfig = KeyspaceConfig()

  implicit val configReaderKeyspace: ConfigReader[KeyspaceConfig] = deriveReader
}
