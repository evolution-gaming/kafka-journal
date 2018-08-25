package com.evolutiongaming.kafka.journal.eventual.cassandra

import com.typesafe.config.ConfigFactory
import org.scalatest.{FunSuite, Matchers}

class EventualCassandraConfigSpec extends FunSuite with Matchers {

  test("apply from empty config") {
    val config = ConfigFactory.empty()
    EventualCassandraConfig(config) shouldEqual EventualCassandraConfig.Default
  }

  test("apply from config") {
    val config = ConfigFactory.parseURL(getClass.getResource("eventual-cassandra.conf"))
    val expected = EventualCassandraConfig(
      retries = 1,
      segmentSize = 2)
    EventualCassandraConfig(config) shouldEqual expected
  }
}