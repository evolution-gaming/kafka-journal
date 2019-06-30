package com.evolutiongaming.kafka.journal.replicator


import cats.Id
import com.evolutiongaming.kafka.journal.replicator.TopicReplicator.Metrics
import com.evolutiongaming.kafka.journal.util.TestSync
import com.evolutiongaming.skafka.Partition
import io.prometheus.client.CollectorRegistry
import org.scalatest.{FunSuite, Matchers}
import scala.concurrent.duration._

class TopicReplicatorMetricsSpec extends FunSuite with Matchers {
  import TopicReplicatorMetricsSpec._

  test("append") {
    new Scope {
      metrics.append(events = 30, bytes = 1000, measurements = measurements)
      registry.replicationLatency(name = "append") shouldEqual Some(2)
      registry.deliveryLatency(name = "append") shouldEqual Some(1)
      registry.events() shouldEqual Some(30)
      registry.bytes() shouldEqual Some(1000)
      registry.records() shouldEqual Some(10)
    }
  }

  test("delete") {
    new Scope {
      metrics.delete(measurements)
      registry.replicationLatency(name = "delete") shouldEqual Some(2)
      registry.deliveryLatency(name = "delete") shouldEqual Some(1)
      registry.records() shouldEqual Some(10)
    }
  }

  test("round") {
    new Scope {
      metrics.round(latency = 1000.millis, records = 10)
      registry.roundDuration() shouldEqual Some(1)
      registry.roundRecords() shouldEqual Some(10)
    }
  }

  private trait Scope {
    val registry = new CollectorRegistry()
    implicit val syncId = TestSync[Id](cats.catsInstancesForId)
    val metrics = TopicReplicatorMetrics.of[Id](registry, prefix).apply(topic)
  }
}

object TopicReplicatorMetricsSpec {

  val prefix = "test"
  val topic = "topic"
  val partition = Partition.Min

  val measurements = Metrics.Measurements(
    partition = partition,
    replicationLatency = 2000.millis,
    deliveryLatency = 1000.millis,
    records = 10)

  implicit class CollectorRegistryOps(val self: CollectorRegistry) extends AnyVal {

    def replicationLatency(name: String) = Option {
      self.getSampleValue(
        s"${ prefix }_replication_latency_sum",
        Array("topic", "partition", "type"),
        Array(topic, partition.toString, name))
    }

    def deliveryLatency(name: String) = Option {
      self.getSampleValue(
        s"${ prefix }_delivery_latency_sum",
        Array("topic", "partition", "type"),
        Array(topic, partition.toString, name))
    }

    def roundDuration() = Option {
      self.getSampleValue(
        s"${ prefix }_round_duration_sum",
        Array("topic"),
        Array(topic))
    }

    def roundRecords() = Option {
      self.getSampleValue(
        s"${ prefix }_round_records_sum",
        Array("topic"),
        Array(topic))
    }

    def events() = Option {
      self.getSampleValue(
        s"${ prefix }_events_sum",
        Array("topic", "partition"),
        Array(topic, partition.toString))
    }

    def bytes() = Option {
      self.getSampleValue(
        s"${ prefix }_bytes_sum",
        Array("topic", "partition"),
        Array(topic, partition.toString))
    }

    def records() = Option {
      self.getSampleValue(
        s"${ prefix }_records_sum",
        Array("topic", "partition"),
        Array(topic, partition.toString))
    }
  }
}

