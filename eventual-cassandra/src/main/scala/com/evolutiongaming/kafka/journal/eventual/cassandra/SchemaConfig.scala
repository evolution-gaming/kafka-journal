package com.evolutiongaming.kafka.journal.eventual.cassandra

import com.evolutiongaming.scassandra.ReplicationStrategyConfig
import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

final case class SchemaConfig(
  keyspace: SchemaConfig.Keyspace = SchemaConfig.Keyspace.default,
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

  @deprecated(since = "3.3.9", message = "Use [[KeyspaceConfig]] instead")
  final case class Keyspace(
    name: String = "journal",
    replicationStrategy: ReplicationStrategyConfig = ReplicationStrategyConfig.Default,
    autoCreate: Boolean = true) {
    
    def toKeyspaceConfig: KeyspaceConfig = KeyspaceConfig(
      name = this.name,
      replicationStrategy = this.replicationStrategy,
      autoCreate = this.autoCreate
    )

  }
  

  @deprecated(since = "3.3.9", message = "Use [[KeyspaceConfig]] instead")
  object Keyspace {

    val default: Keyspace = Keyspace()


    implicit val configReaderKeyspace: ConfigReader[Keyspace] = deriveReader
  }

}
