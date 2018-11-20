package com.evolutiongaming.kafka.journal.replicator

import com.evolutiongaming.kafka.journal.IO
import com.evolutiongaming.kafka.journal.eventual.ReplicatedJournal
import com.evolutiongaming.kafka.journal.replicator.MetricsHelper._
import com.evolutiongaming.skafka.Topic
import io.prometheus.client.{CollectorRegistry, Summary}

object ReplicatedJournalMetrics {

  def apply[F[_] : IO](
    registry: CollectorRegistry,
    prefix: String = "replicated_journal"): ReplicatedJournal.Metrics[F] = {

    val latencySummary = Summary.build()
      .name(s"${ prefix }_latency")
      .help("Journal call latency in seconds")
      .labelNames("type")
      .quantile(0.5, 0.05)
      .quantile(0.9, 0.05)
      .quantile(0.95, 0.01)
      .quantile(0.99, 0.005)
      .register(registry)

    val topicLatencySummary = Summary.build()
      .name(s"${ prefix }_topic_latency")
      .help("Journal topic call latency in seconds")
      .labelNames("topic", "type")
      .quantile(0.5, 0.05)
      .quantile(0.9, 0.05)
      .quantile(0.95, 0.01)
      .quantile(0.99, 0.005)
      .register(registry)

    val eventsSummary = Summary.build()
      .name(s"${ prefix }_events")
      .help("Number of events saved")
      .labelNames("topic")
      .register(registry)

    def observeTopicLatency(name: String, topic: Topic, latency: Long) = {
      topicLatencySummary
        .labels(topic, name)
        .observe(latency.toSeconds)
    }

    def observeLatency(name: String, latency: Long) = {
      latencySummary
        .labels(name)
        .observe(latency.toSeconds)
    }

    new ReplicatedJournal.Metrics[F] {

      def topics(latency: Long) = {
        IO[F].effect {
          observeLatency(name = "topics", latency = latency)
        }
      }

      def pointers(latency: Long) = {
        IO[F].effect {
          observeLatency(name = "pointers", latency = latency)
        }
      }

      def append(topic: Topic, latency: Long, events: Int) = {
        IO[F].effect {
          eventsSummary
            .labels(topic)
            .observe(events.toDouble)
          observeTopicLatency(name = "append", topic = topic, latency = latency)
        }
      }

      def delete(topic: Topic, latency: Long) = {
        IO[F].effect {
          observeTopicLatency(name = "delete", topic = topic, latency = latency)
        }
      }

      def save(topic: Topic, latency: Long) = {
        IO[F].effect {
          observeTopicLatency(name = "save", topic = topic, latency = latency)
        }
      }
    }
  }
}
