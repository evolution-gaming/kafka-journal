package com.evolutiongaming.kafka.journal

import com.evolutiongaming.kafka.journal.MetricsHelper._
import com.evolutiongaming.kafka.journal.eventual.EventualJournal
import com.evolutiongaming.skafka.Topic
import io.prometheus.client.{CollectorRegistry, Summary}

object EventualJournalMetrics {

  def apply[F[_]](
    registry: CollectorRegistry,
    prefix: String = "eventual_journal")(implicit unit: F[Unit]): EventualJournal.Metrics[F] = {

    val latencySummary = Summary.build()
      .name(s"${ prefix }_topic_latency")
      .help("Journal call latency in seconds")
      .labelNames("topic", "type")
      .quantile(0.5, 0.05)
      .quantile(0.9, 0.01)
      .quantile(0.99, 0.001)
      .register(registry)

    val eventsSummary = Summary.build()
      .name(s"${ prefix }_events")
      .help("Number of events")
      .labelNames("topic")
      .register(registry)

    def observeLatency(name: String, topic: Topic, latency: Long) = {
      latencySummary
        .labels(topic, name)
        .observe(latency.toSeconds)
      unit
    }

    new EventualJournal.Metrics[F] {

      def pointers(topic: Topic, latency: Long) = {
        observeLatency(name = "pointers", topic = topic, latency = latency)
      }

      def read(topic: Topic, latency: Long, events: Int) = {
        eventsSummary
          .labels(topic)
          .observe(events.toDouble)
        observeLatency(name = "read", topic = topic, latency = latency)
      }

      def pointer(topic: Topic, latency: Long) = {
        observeLatency(name = "pointer", topic = topic, latency = latency)
      }
    }
  }
}
