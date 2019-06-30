package com.evolutiongaming.kafka.journal

import com.evolutiongaming.kafka.journal.util.TestSync
import io.prometheus.client.CollectorRegistry
import org.scalatest.{FunSuite, Matchers}
import scala.concurrent.duration._

class HeadCacheMetricsSpec extends FunSuite with Matchers {
  import HeadCacheMetricsSpec._

  test("get") {
    new Scope {
      metrics.get(topic, latency = 1000.millis, result = HeadCache.Metrics.Result.Replicated)
      registry.latency("replicated") shouldEqual Some(1)
      registry.result("replicated") shouldEqual Some(1)
    }
  }

  test("listeners") {
    new Scope {
      metrics.listeners(topic, 100)
      registry.listeners shouldEqual Some(100)
    }
  }

  test("round") {
    new Scope {
      metrics.round(topic, entries = 100, listeners = 10, deliveryLatency = 100.millis)
      registry.entries shouldEqual Some(100)
      registry.listeners shouldEqual Some(10)
      registry.deliveryLatency shouldEqual Some(0.1)
    }
  }

  private trait Scope {
    implicit val syncId = TestSync[cats.Id](cats.catsInstancesForId)
    val registry = new CollectorRegistry()
    val metrics = HeadCacheMetrics.of[cats.Id](registry, prefix)
  }
}

object HeadCacheMetricsSpec {

  val prefix = "test"
  val topic = "topic"

  implicit class CollectorRegistryOps(val self: CollectorRegistry) extends AnyVal {

    def latency(name: String): Option[java.lang.Double] = Option {
      self.getSampleValue(
        s"${ prefix }_get_latency_sum",
        Array("topic", "result"),
        Array(topic, name))
    }

    def result(name: String): Option[java.lang.Double] = Option {
      self.getSampleValue(
        s"${ prefix }_get_results",
        Array("topic", "result"),
        Array(topic, name))
    }

    def entries: Option[java.lang.Double] = Option {
      self.getSampleValue(
        s"${ prefix }_entries",
        Array("topic"),
        Array(topic))
    }

    def listeners: Option[java.lang.Double] = Option {
      self.getSampleValue(
        s"${ prefix }_listeners",
        Array("topic"),
        Array(topic))
    }

    def deliveryLatency: Option[java.lang.Double] = Option {
      self.getSampleValue(
        s"${ prefix }_delivery_latency_sum",
        Array("topic"),
        Array(topic))
    }
  }
}



