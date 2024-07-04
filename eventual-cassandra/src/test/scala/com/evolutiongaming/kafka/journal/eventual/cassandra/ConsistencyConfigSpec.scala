package com.evolutiongaming.kafka.journal.eventual.cassandra

import com.datastax.driver.core.ConsistencyLevel
import com.evolutiongaming.kafka.journal.cassandra.CassandraConsistencyConfigSpec
import com.evolutiongaming.kafka.journal.eventual.cassandra.EventualCassandraConfig.ConsistencyConfig
import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import pureconfig.ConfigSource

class ConsistencyConfigSpec extends AnyFunSuite with Matchers {

  test("apply from empty config") {
    val actualConfig = ConfigSource.empty.load[ConsistencyConfig]
    val expectedConfig = ConsistencyConfig.default
    assert(actualConfig == Right(expectedConfig))
  }

  test("apply from config") {
    val url = classOf[CassandraConsistencyConfigSpec].getResource("cassandra-consistency-config.conf")
    val config = ConfigFactory.parseURL(url)

    val actualConfig = ConfigSource.fromConfig(config).load[ConsistencyConfig]

    val expectedConfig = ConsistencyConfig(
      read = ConsistencyConfig.Read(ConsistencyLevel.QUORUM),
      write = ConsistencyConfig.Write(ConsistencyLevel.EACH_QUORUM)
    )
      
    assert(actualConfig == Right(expectedConfig))
  }
}
