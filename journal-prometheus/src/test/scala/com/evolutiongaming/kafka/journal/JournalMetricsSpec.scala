package com.evolutiongaming.kafka.journal

import io.prometheus.client.CollectorRegistry
import org.scalatest.{FunSuite, Matchers}

class JournalMetricsSpec extends FunSuite with Matchers {
  import JournalMetricsSpec._

  // TODO duplicate
  type Id[A] = A

  test("append") {
    new Scope {
      metrics.append(topic, latency = 1000, events = 10)
      registry.latency("append") shouldEqual Some(1)
      registry.events("append") shouldEqual Some(10)
    }
  }

  test("read") {
    new Scope {
      metrics.read(topic, latency = 1000, events = 10)
      registry.latency("read") shouldEqual Some(1)
      registry.events("read") shouldEqual Some(10)
    }
  }

  test("pointer") {
    new Scope {
      metrics.pointer(topic, 1000)
      registry.latency("pointer") shouldEqual Some(1)
    }
  }

  test("delete") {
    new Scope {
      metrics.delete(topic, 1000)
      registry.latency("delete") shouldEqual Some(1)
    }
  }

  private trait Scope {
    val registry = new CollectorRegistry()
    val metrics = JournalMetrics[Id](registry, prefix)(())
  }
}

object JournalMetricsSpec {

  val prefix = "test"
  val topic = "topic"

  implicit class CollectorRegistryOps(val self: CollectorRegistry) extends AnyVal {

    def latency(name: String) = Option {
      self.getSampleValue(
        s"${ prefix }_topic_latency_sum",
        Array("topic", "type"),
        Array(topic, name))
    }

    def events(name: String) = Option {
      self.getSampleValue(
        s"${ prefix }_events_sum",
        Array("topic", "type"),
        Array(topic, name))
    }
  }
}


