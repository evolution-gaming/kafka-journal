package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.syntax.all.*
import com.datastax.driver.core.ConsistencyLevel
import com.evolutiongaming.kafka.journal.cassandra.CassandraConsistencyConfig
import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import pureconfig.ConfigSource

class EventualCassandraConfigSpec extends AnyFunSuite with Matchers {

  test("apply from empty config") {
    val config = ConfigFactory.empty()
    val expected = EventualCassandraConfig.default
    ConfigSource.fromConfig(config).load[EventualCassandraConfig] shouldEqual expected.asRight
  }

  test("apply from config") {
    val config = ConfigFactory.parseURL(getClass.getResource("eventual-cassandra.conf"))
    val expected = EventualCassandraConfig(
      retries = 1,
      segmentSize = SegmentSize.min,
      consistencyConfig = CassandraConsistencyConfig
        .default
        .copy(
          read = CassandraConsistencyConfig.Read(ConsistencyLevel.QUORUM),
        ),
    )
    ConfigSource.fromConfig(config).load[EventualCassandraConfig] shouldEqual expected.asRight
  }
}
