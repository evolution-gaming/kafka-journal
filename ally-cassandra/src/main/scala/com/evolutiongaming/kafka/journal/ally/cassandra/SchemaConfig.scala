package com.evolutiongaming.kafka.journal.ally.cassandra

import com.evolutiongaming.cassandra.ReplicationStrategyConfig

case class SchemaConfig(
  keyspace: SchemaConfig.Keyspace = SchemaConfig.Keyspace.Default,
  table: String = "journal",
  autoCreate: Boolean = true)

// TODO parse config
object SchemaConfig {

  val Default: SchemaConfig = SchemaConfig()

  case class Keyspace(
    name: String = "journal",
    replicationStrategy: ReplicationStrategyConfig = ReplicationStrategyConfig.Default,
    autoCreate: Boolean = true)

  object Keyspace {

    val Default: Keyspace = Keyspace()
  }
}