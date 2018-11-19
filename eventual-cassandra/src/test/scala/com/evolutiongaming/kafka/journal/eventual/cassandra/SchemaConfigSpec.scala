package com.evolutiongaming.kafka.journal.eventual.cassandra

import com.evolutiongaming.scassandra.ReplicationStrategyConfig
import com.typesafe.config.ConfigFactory
import org.scalatest.{FunSuite, Matchers}


class SchemaConfigSpec extends FunSuite with Matchers {

  test("apply from empty config") {
    val config = ConfigFactory.empty()
    SchemaConfig(config) shouldEqual SchemaConfig.Default
  }

  test("apply from config") {
    val config = ConfigFactory.parseURL(getClass.getResource("schema.conf"))
    val expected = SchemaConfig(
      keyspace = SchemaConfig.Keyspace(
        name = "keyspace",
        replicationStrategy = ReplicationStrategyConfig.Simple(3),
        autoCreate = false),
      journalTable = "table-journal",
      metadataTable = "table-metadata",
      pointerTable = "table-pointer",
      locksTable = "table-locks",
      autoCreate = false)
    SchemaConfig(config) shouldEqual expected
  }
}
