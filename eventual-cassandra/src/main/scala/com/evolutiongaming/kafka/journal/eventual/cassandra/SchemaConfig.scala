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
  settingTable: String = "setting",
  locksTable: String = "locks",
  autoCreate: Boolean = true
)

object SchemaConfig {

  val default: SchemaConfig = SchemaConfig()

  implicit val configReaderSchemaConfig: ConfigReader[SchemaConfig] = deriveReader


  @deprecated(since = "3.2.2", message = "Use [[KeyspaceConfig]] instead")
  type Keyspace = Nothing

}
