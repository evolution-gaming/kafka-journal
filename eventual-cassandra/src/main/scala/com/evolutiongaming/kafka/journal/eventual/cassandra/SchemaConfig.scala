package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.implicits._
import com.evolutiongaming.config.ConfigHelper._
import com.evolutiongaming.scassandra.ReplicationStrategyConfig
import com.typesafe.config.Config
import pureconfig.{ConfigCursor, ConfigReader}
import pureconfig.error.{ConfigReaderFailures, ThrowableFailure}

import scala.util.Try

// make a part of EventualCassandraConfig
final case class SchemaConfig(
  keyspace: SchemaConfig.Keyspace = SchemaConfig.Keyspace.default,
  journalTable: String = "journal",
  headTable: String = "metadata",
  pointerTable: String = "pointer",
  settingTable: String = "setting",
  locksTable: String = "locks",
  autoCreate: Boolean = true)


object SchemaConfig {

  val default: SchemaConfig = SchemaConfig()


  implicit val configReaderSchemaConfig: ConfigReader[SchemaConfig] = {
    cursor: ConfigCursor => {
      for {
        cursor  <- cursor.asObjectCursor
        journal  = Try { apply1(cursor.value.toConfig, default) }
        journal <- journal.toEither.leftMap(a => ConfigReaderFailures(ThrowableFailure(a, cursor.location)))
      } yield journal
    }
  }


  @deprecated("use ConfigReader instead", "0.0.87")
  def apply(config: Config): SchemaConfig = apply1(config, default)

  @deprecated("use ConfigReader instead", "0.0.87")
  def apply(config: Config, default: => SchemaConfig): SchemaConfig = apply1(config, default)


  def apply1(config: Config, default: => SchemaConfig): SchemaConfig = {

    def get[T: FromConf](name: String) = config.getOpt[T](name)

    SchemaConfig(
      keyspace = get[Config]("keyspace").fold(default.keyspace)(Keyspace.apply1(_, default.keyspace)),
      journalTable = get[String]("journal-table") getOrElse default.journalTable,
      headTable = get[String]("head-table") getOrElse default.headTable,
      pointerTable = get[String]("pointer-table") getOrElse default.pointerTable,
      settingTable = get[String]("setting-table") getOrElse default.settingTable,
      locksTable = get[String]("locks-table") getOrElse default.locksTable,
      autoCreate = get[Boolean]("auto-create") getOrElse default.autoCreate)
  }


  final case class Keyspace(
    name: String = "journal",
    replicationStrategy: ReplicationStrategyConfig = ReplicationStrategyConfig.Default,
    autoCreate: Boolean = true)

  object Keyspace {

    val default: Keyspace = Keyspace()


    implicit val configReaderKeyspace: ConfigReader[Keyspace] = {
      cursor: ConfigCursor => {
        for {
          cursor  <- cursor.asObjectCursor
          journal  = Try { apply1(cursor.value.toConfig, default) }
          journal <- journal.toEither.leftMap(a => ConfigReaderFailures(ThrowableFailure(a, cursor.location)))
        } yield journal
      }
    }


    @deprecated("use ConfigReader instead", "0.0.87")
    def apply(config: Config): Keyspace = apply1(config, default)

    @deprecated("use ConfigReader instead", "0.0.87")
    def apply(config: Config, default: => Keyspace): Keyspace = apply1(config, default)

    
    def apply1(config: Config, default: => Keyspace): Keyspace = {

      def get[T: FromConf](name: String) = config.getOpt[T](name)

      Keyspace(
        name = get[String]("name") getOrElse default.name,
        replicationStrategy = ReplicationStrategyConfig(config),
        autoCreate = get[Boolean]("auto-create") getOrElse default.autoCreate)
    }
  }
}