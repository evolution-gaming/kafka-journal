package com.evolutiongaming.kafka.journal.eventual.cassandra

import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader


final case class SchemaConfig(
  keyspace: KeyspaceConfig = KeyspaceConfig.default,
  journalTable: String = "journal",
  metadataTable: String = "metadata",
  metaJournalTable: String = "metajournal",
  pointerTable: String = "pointer",
  pointer2Table: String = "pointer2",
  snapshotTable: String = "snapshot_buffer",
  settingTable: String = "setting",
  locksTable: String = "locks",
  autoCreate: Boolean = true)


object SchemaConfig {

  val default: SchemaConfig = SchemaConfig()

  implicit val configReaderSchemaConfig: ConfigReader[SchemaConfig] = deriveReader

}
