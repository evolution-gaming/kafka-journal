package com.evolutiongaming.kafka.journal.snapshot.cassandra

import com.evolutiongaming.kafka.journal.cassandra.KeyspaceConfig
import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

final case class SnapshotSchemaConfig(
  keyspace: KeyspaceConfig = KeyspaceConfig.default.copy(name = "snapshot"),
  snapshotTable: String = "snapshot_buffer",
  settingTable: String = "setting",
  locksTable: String = "locks",
  autoCreate: Boolean = true
)

object SnapshotSchemaConfig {

  val default: SnapshotSchemaConfig = SnapshotSchemaConfig()

  implicit val configReaderSchemaConfig: ConfigReader[SnapshotSchemaConfig] = deriveReader

}
