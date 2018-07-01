package com.evolutiongaming.kafka.journal.ally.cassandra

import com.typesafe.config.ConfigFactory
import org.scalatest.{FunSuite, Matchers}

class AllyCassandraConfigSpec extends FunSuite with Matchers {

  test("apply from empty config") {
    val config = ConfigFactory.empty()
    AllyCassandraConfig(config) shouldEqual AllyCassandraConfig.Default
  }

  test("apply from config") {
    val config = ConfigFactory.parseURL(getClass.getResource("ally-cassandra.conf"))
    val expected = AllyCassandraConfig(
      segmentSize = 1)
    AllyCassandraConfig(config) shouldEqual expected
  }
}