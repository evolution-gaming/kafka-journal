package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.implicits._
import com.typesafe.config.ConfigFactory
import org.scalatest.{FunSuite, Matchers}
import pureconfig.ConfigSource

class EventualCassandraConfigSpec extends FunSuite with Matchers {

  test("apply from empty config") {
    val config = ConfigFactory.empty()
    val expected = EventualCassandraConfig.default
    ConfigSource.fromConfig(config).load[EventualCassandraConfig] shouldEqual expected.asRight
  }

  test("apply from config") {
    val config = ConfigFactory.parseURL(getClass.getResource("eventual-cassandra.conf"))
    val expected = EventualCassandraConfig(
      retries = 1,
      segmentSize = 2)
    ConfigSource.fromConfig(config).load[EventualCassandraConfig] shouldEqual expected.asRight
  }
}