package com.evolutiongaming.kafka.journal.replicator

import io.prometheus.client.CollectorRegistry
import org.scalatest.{FunSuite, Matchers}
import cats.Id
import com.evolutiongaming.kafka.journal.util.TestSync

class ReplicatedJournalMetricsSpec extends FunSuite with Matchers {
  import ReplicatedJournalMetricsSpec._

  test("topics") {
    new Scope {
      metrics.topics(1000)
      registry.latency("topics") shouldEqual Some(1)
    }
  }

  test("pointers") {
    new Scope {
      metrics.pointers(1000)
      registry.latency("pointers") shouldEqual Some(1)
    }
  }

  test("append") {
    new Scope {
      metrics.append(topic, 1000, 1)
      registry.topicLatency("append") shouldEqual Some(1)
      registry.events() shouldEqual Some(1)
    }
  }

  test("delete") {
    new Scope {
      metrics.delete(topic, 1000)
      registry.topicLatency("delete") shouldEqual Some(1)
    }
  }

  test("save") {
    new Scope {
      metrics.save(topic, 1000)
      registry.topicLatency("save") shouldEqual Some(1)
    }
  }

  private trait Scope {
    val registry = new CollectorRegistry()
    implicit val syncId = TestSync[Id](cats.catsInstancesForId)
    val metrics = ReplicatedJournalMetrics.of[Id](registry, prefix)
  }
}

object ReplicatedJournalMetricsSpec {

  val prefix = "test"
  val topic = "topic"

  implicit class CollectorRegistryOps(val self: CollectorRegistry) extends AnyVal {

    def latency(name: String) = Option {
      self.getSampleValue(
        s"${ prefix }_latency_sum",
        Array("type"),
        Array(name))
    }

    def topicLatency(name: String) = Option {
      self.getSampleValue(
        s"${ prefix }_topic_latency_sum",
        Array("topic", "type"),
        Array(topic, name))
    }

    def events() = Option {
      self.getSampleValue(
        s"${ prefix }_events_sum",
        Array("topic"),
        Array(topic))
    }
  }
}
