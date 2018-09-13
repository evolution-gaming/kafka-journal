package com.evolutiongaming.kafka.journal.replicator

import com.datastax.driver.core.ConsistencyLevel
import com.evolutiongaming.cassandra.{CassandraConfig, QueryConfig}
import com.evolutiongaming.kafka.journal.eventual.cassandra.EventualCassandraConfig
import com.evolutiongaming.nel.Nel
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
      topicPrefixes = Nel("prefix1", "prefix2"),
      topicDiscoveryInterval = 1.minute,
      pollTimeout = 200.millis)
    ReplicatorConfig(config) shouldEqual expected
  }

  test("apply from config with common kafka") {
    val config = ConfigFactory.parseURL(getClass.getResource("replicator_kafka.conf"))
    val expected = ReplicatorConfig(
      topicPrefixes = Nel("prefix"),
      consumer = ConsumerConfig(
        maxPollRecords = 10,
        common = CommonConfig(clientId = Some("clientId"))))
    ReplicatorConfig(config) shouldEqual expected
  }

  // TODO implement the same for persistence journal
  test("apply from reference.conf") {
    val config = ConfigFactory.load().getConfig("evolutiongaming.kafka-journal.replicator")
    val expected = ReplicatorConfig(
      consumer = ConsumerConfig(
        common = CommonConfig(clientId = Some("replicator")),
        groupId = Some("replicator"),
        maxPollRecords = 200,
        autoCommit = false,
        autoOffsetReset = AutoOffsetReset.Earliest),
      cassandra = EventualCassandraConfig(
        client = CassandraConfig(
          name = "replicator",
          query = QueryConfig(
            consistency = ConsistencyLevel.LOCAL_QUORUM,
            defaultIdempotence = true))))
    ReplicatorConfig(config) shouldEqual expected
  }
}