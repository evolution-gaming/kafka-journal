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
  pointerTable2: String = "pointer2",
  settingTable: String = "setting",
  locksTable: String = "locks",
  autoCreate: Boolean = true)


object SchemaConfig {

  val default: SchemaConfig = SchemaConfig()

  implicit val configReaderSchemaConfig: ConfigReader[SchemaConfig] = deriveReader


  final case class Keyspace(
    name: String = "journal",
    replicationStrategy: ReplicationStrategyConfig = ReplicationStrategyConfig.Default,
    autoCreate: Boolean = true)

  object Keyspace {

    val default: Keyspace = Keyspace()


    implicit val configReaderKeyspace: ConfigReader[Keyspace] = deriveReader
  }
}