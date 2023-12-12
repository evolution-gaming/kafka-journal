package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.syntax.all._
import com.evolutiongaming.scassandra.ReplicationStrategyConfig
import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import pureconfig.ConfigSource


class SchemaConfigSpec extends AnyFunSuite with Matchers {

  test("apply from empty config") {
    val config = ConfigFactory.empty()
    ConfigSource.fromConfig(config).load[SchemaConfig] shouldEqual SchemaConfig.default.asRight
  }

  test("apply from config") {
    val config = ConfigFactory.parseURL(getClass.getResource("schema.conf"))
    val expected = SchemaConfig(
      keyspace = KeyspaceConfig(
        name = "keyspace",
        replicationStrategy = ReplicationStrategyConfig.Simple(3),
        autoCreate = false),
      journalTable = "table-journal",
      metadataTable = "table-metadata",
      metaJournalTable = "table-meta-journal",
      pointerTable = "table-pointer",
      settingTable = "table-setting",
      locksTable = "table-locks",
      autoCreate = false)
    ConfigSource.fromConfig(config).load[SchemaConfig] shouldEqual expected.asRight
  }
}
