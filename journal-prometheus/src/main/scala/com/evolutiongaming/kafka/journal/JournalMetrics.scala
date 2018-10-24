package com.evolutiongaming.kafka.journal

import com.evolutiongaming.kafka.journal.MetricsHelper._
import com.evolutiongaming.skafka.Topic
import io.prometheus.client.{CollectorRegistry, Summary}

object JournalMetrics {

  def apply[F[_]](
    registry: CollectorRegistry,
    prefix: String = "journal")(implicit unit: F[Unit]): Journal.Metrics[F] = {

    val latencySummary = Summary.build()
      .name(s"${ prefix }_topic_latency")
      .help("Journal call latency in seconds")
      .labelNames("topic", "type")
      .quantile(0.5, 0.05)
      .quantile(0.9, 0.05)
      .quantile(0.95, 0.01)
      .quantile(0.99, 0.005)
      .register(registry)

    val eventsSummary = Summary.build()
      .name(s"${ prefix }_events")
      .help("Number of events")
      .labelNames("topic", "type")
      .register(registry)

    def observeLatency(name: String, topic: Topic, latency: Long) = {
      latencySummary
        .labels(topic, name)
        .observe(latency.toSeconds)
    }

    def observeEvents(name: String, topic: Topic, events: Int) = {
      eventsSummary
        .labels(topic, name)
        .observe(events.toDouble)
    }

    new Journal.Metrics[F] {

      def append(topic: Topic, latency: Long, events: Int) = {
        observeEvents(name = "append", topic = topic, events = events)
        observeLatency(name = "append", topic = topic, latency = latency)
        unit
      }

      def read(topic: Topic, latency: Long, events: Int) = {
        observeEvents(name = "read", topic = topic, events = events)
        observeLatency(name = "read", topic = topic, latency = latency)
        unit
      }

      def pointer(topic: Topic, latency: Long) = {
        observeLatency(name = "pointer", topic = topic, latency = latency)
        unit
      }

      def delete(topic: Topic, latency: Long) = {
        observeLatency(name = "delete", topic = topic, latency = latency)
        unit
      }
    }
  }
}
