package com.evolutiongaming.kafka.journal.eventual.cassandra

import com.evolutiongaming.config.ConfigHelper._
import com.evolutiongaming.scassandra.ReplicationStrategyConfig
import com.typesafe.config.Config

// make a part of EventualCassandraConfig
final case class SchemaConfig(
  keyspace: SchemaConfig.Keyspace = SchemaConfig.Keyspace.Default,
  journalTable: String = "journal",
  metadataTable: String = "metadata",
  pointerTable: String = "pointer",
  autoCreate: Boolean = true)


object SchemaConfig {

  val Default: SchemaConfig = SchemaConfig()


  def apply(config: Config): SchemaConfig = {

    def get[T: FromConf](name: String) = config.getOpt[T](name)

    SchemaConfig(
      keyspace = get[Config]("keyspace").fold(Default.keyspace)(Keyspace.apply),
      journalTable = get[String]("journal-table") getOrElse Default.journalTable,
      metadataTable = get[String]("metadata-table") getOrElse Default.metadataTable,
      pointerTable = get[String]("pointer-table") getOrElse Default.pointerTable,
      autoCreate = get[Boolean]("auto-create") getOrElse Default.autoCreate)
  }


  final case class Keyspace(
    name: String = "journal",
    replicationStrategy: ReplicationStrategyConfig = ReplicationStrategyConfig.Default,
    autoCreate: Boolean = true)

  object Keyspace {

    val Default: Keyspace = Keyspace()

    def apply(config: Config): Keyspace = {

      def get[T: FromConf](name: String) = config.getOpt[T](name)

      Keyspace(
        name = get[String]("name") getOrElse Default.name,
        replicationStrategy = ReplicationStrategyConfig(config),
        autoCreate = get[Boolean]("auto-create") getOrElse Default.autoCreate)
    }
  }
}