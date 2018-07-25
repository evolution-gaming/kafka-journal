package com.evolutiongaming.kafka.journal.replicator

import com.evolutiongaming.nel.Nel
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
      topicDiscoveryInterval = 1.minute)
    ReplicatorConfig(config) shouldEqual expected
  }
}