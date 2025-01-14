package com.evolutiongaming.kafka.journal.eventual.cassandra

import com.evolutiongaming.kafka.journal.cassandra.KeyspaceConfig
import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

private[journal] final case class SchemaConfig(
    keyspace: KeyspaceConfig = KeyspaceConfig.default,
    journalTable: String     = "journal",
    metaJournalTable: String = "metajournal",
    pointerTable: String     = "pointer", // should not be used any more
    pointer2Table: String    = "pointer2",
    settingTable: String     = "setting",
    locksTable: String       = "locks",
    autoCreate: Boolean      = true,
)

private[journal] object SchemaConfig {

  val default: SchemaConfig = SchemaConfig()

  implicit val configReaderSchemaConfig: ConfigReader[SchemaConfig] = deriveReader
}
