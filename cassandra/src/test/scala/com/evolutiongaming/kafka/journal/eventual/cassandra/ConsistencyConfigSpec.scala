package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.syntax.all._
import com.datastax.driver.core.ConsistencyLevel
import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import pureconfig.ConfigSource

class ConsistencyConfigSpec extends AnyFunSuite with Matchers {

  test("apply from empty config") {
    ConfigSource
      .empty
      .load[CassandraConsistencyConfig] shouldEqual CassandraConsistencyConfig.default.asRight
  }

  test("apply from config") {
    val config = ConfigFactory.parseURL(getClass.getResource("consistency-config.conf"))
    val expected = CassandraConsistencyConfig(
      CassandraConsistencyConfig.Read(ConsistencyLevel.QUORUM),
      CassandraConsistencyConfig.Write(ConsistencyLevel.EACH_QUORUM))

    ConfigSource
      .fromConfig(config)
      .load[CassandraConsistencyConfig] shouldEqual expected.asRight
  }
}
