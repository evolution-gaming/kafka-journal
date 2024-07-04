package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.syntax.all._
import com.datastax.driver.core.ConsistencyLevel
import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import pureconfig.ConfigSource

class EventualCassandraConfigSpec extends AnyFunSuite with Matchers {

  test("apply from empty config") {
    val config   = ConfigFactory.empty()
    val expected = EventualCassandraConfig.default
    ConfigSource.fromConfig(config).load[EventualCassandraConfig] shouldEqual expected.asRight
  }

  test("apply from config") {
    val config = ConfigFactory.parseURL(getClass.getResource("eventual-cassandra.conf"))
    val expected = EventualCassandraConfig(
      retries     = 1,
      segmentSize = SegmentSize.min,
      consistencyConfig = EventualCassandraConfig
        .ConsistencyConfig
        .default
        .copy(
          read = EventualCassandraConfig.ConsistencyConfig.Read(ConsistencyLevel.QUORUM),
        ),
    )
    ConfigSource.fromConfig(config).load[EventualCassandraConfig] shouldEqual expected.asRight
  }
}
