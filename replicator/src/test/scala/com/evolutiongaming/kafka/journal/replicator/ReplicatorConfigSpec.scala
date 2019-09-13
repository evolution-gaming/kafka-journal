package com.evolutiongaming.kafka.journal.replicator

import cats.data.{NonEmptyList => Nel}
import com.datastax.driver.core.ConsistencyLevel
import com.evolutiongaming.kafka.journal.eventual.cassandra.EventualCassandraConfig
import com.evolutiongaming.scassandra.{CassandraConfig, QueryConfig}
import com.evolutiongaming.skafka.CommonConfig
import com.evolutiongaming.skafka.consumer.{AutoOffsetReset, ConsumerConfig}
import com.typesafe.config.ConfigFactory
import org.scalatest.{FunSuite, Matchers}

import scala.concurrent.duration._


class ReplicatorConfigSpec extends FunSuite with Matchers {

  test("apply from empty config") {
    val config = ConfigFactory.empty()
    ReplicatorConfig(config) shouldEqual ReplicatorConfig.Default
  }

  test("apply from config") {
    val config = ConfigFactory.parseURL(getClass.getResource("replicator.conf"))
    val expected = ReplicatorConfig(
      topicPrefixes = Nel.of("prefix1", "prefix2"),
      topicDiscoveryInterval = 1.minute,
      pollTimeout = 200.millis)
    ReplicatorConfig(config) shouldEqual expected
  }

  test("apply from config with common kafka") {
    val config = ConfigFactory.parseURL(getClass.getResource("replicator_kafka.conf"))
    val expected = ReplicatorConfig(
      topicPrefixes = Nel.of("prefix"),
      consumer = ConsumerConfig(
        maxPollRecords = 10,
        common = CommonConfig(
          clientId = Some("clientId"),
          receiveBufferBytes = 1000000),
        groupId = Some("replicator"),
        autoCommit = false,
        autoOffsetReset = AutoOffsetReset.Earliest))
    ReplicatorConfig(config) shouldEqual expected
  }

  test("apply from reference.conf") {
    val config = ConfigFactory.load().getConfig("evolutiongaming.kafka-journal.replicator")
    val expected = ReplicatorConfig(
      consumer = ConsumerConfig(
        common = CommonConfig(
          clientId = Some("replicator"),
          receiveBufferBytes = 1000000),
        groupId = Some("replicator"),
        autoCommit = false,
        autoOffsetReset = AutoOffsetReset.Earliest,
        maxPollRecords = 1000),
      cassandra = EventualCassandraConfig(
        client = CassandraConfig(
          name = "replicator",
          query = QueryConfig(
            consistency = ConsistencyLevel.LOCAL_QUORUM,
            defaultIdempotence = true))))
    ReplicatorConfig(config) shouldEqual expected
  }
}