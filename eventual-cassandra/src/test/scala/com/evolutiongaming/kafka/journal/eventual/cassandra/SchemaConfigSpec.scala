package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.implicits._
import com.evolutiongaming.scassandra.ReplicationStrategyConfig
import com.typesafe.config.ConfigFactory
import org.scalatest.{FunSuite, Matchers}
import pureconfig.ConfigSource


class SchemaConfigSpec extends FunSuite with Matchers {

  test("apply from empty config") {
    val config = ConfigFactory.empty()
    ConfigSource.fromConfig(config).load[SchemaConfig] shouldEqual SchemaConfig.default.asRight
  }

  test("apply from config") {
    val config = ConfigFactory.parseURL(getClass.getResource("schema.conf"))
    val expected = SchemaConfig(
      keyspace = SchemaConfig.Keyspace(
        name = "keyspace",
        replicationStrategy = ReplicationStrategyConfig.Simple(3),
        autoCreate = false),
      journalTable = "table-journal",
      headTable = "table-head",
      pointerTable = "table-pointer",
      settingTable = "table-setting",
      locksTable = "table-locks",
      autoCreate = false)
    ConfigSource.fromConfig(config).load[SchemaConfig] shouldEqual expected.asRight
  }
}
