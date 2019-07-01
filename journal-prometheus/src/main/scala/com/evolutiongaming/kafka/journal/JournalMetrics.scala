package com.evolutiongaming.kafka.journal

import cats.effect.Sync
import cats.implicits._
import com.evolutiongaming.smetrics.MetricsHelper._
import com.evolutiongaming.skafka.Topic
import io.prometheus.client.{CollectorRegistry, Counter, Summary}

import scala.concurrent.duration.FiniteDuration

object JournalMetrics {

  def of[F[_] : Sync](
    registry: CollectorRegistry,
    prefix: String = "journal"): F[Journal.Metrics[F]] = {

    Sync[F].delay {

      val latencySummary = Summary.build()
        .name(s"${ prefix }_topic_latency")
        .help("Journal call latency in seconds")
        .labelNames("topic", "type")
        .quantile(0.9, 0.05)
        .quantile(0.99, 0.005)
        .register(registry)

      val eventsSummary = Summary.build()
        .name(s"${ prefix }_events")
        .help("Number of events")
        .labelNames("topic", "type")
        .register(registry)

      val resultCounter = Counter.build()
        .name(s"${ prefix }_results")
        .help("Call result: success or failure")
        .labelNames("topic", "type", "result")
        .register(registry)

      def observeLatency(name: String, topic: Topic, latency: FiniteDuration) = {
        Sync[F].delay {
          latencySummary
            .labels(topic, name)
            .observe(latency.toNanos.nanosToSeconds)
          resultCounter
            .labels(topic, name, "success")
            .inc()
        }
      }

      def observeEvents(name: String, topic: Topic, events: Int) = {
        Sync[F].delay {
          eventsSummary
            .labels(topic, name)
            .observe(events.toDouble)
        }
      }

      new Journal.Metrics[F] {

        def append(topic: Topic, latency: FiniteDuration, events: Int) = {
          for {
            _ <- observeEvents(name = "append", topic = topic, events = events)
            _ <- observeLatency(name = "append", topic = topic, latency = latency)
          } yield {}
        }

        def read(topic: Topic, latency: FiniteDuration) = {
          observeLatency(name = "read", topic = topic, latency = latency)
        }

        def read(topic: Topic) = {
          observeEvents(name = "read", topic = topic, events = 1)
        }

        def pointer(topic: Topic, latency: FiniteDuration) = {
          observeLatency(name = "pointer", topic = topic, latency = latency)
        }

        def delete(topic: Topic, latency: FiniteDuration) = {
          observeLatency(name = "delete", topic = topic, latency = latency)
        }

        def failure(name: String, topic: Topic) = {
          Sync[F].delay {
            resultCounter
              .labels(topic, name, "failure")
              .inc()
          }
        }
      }
    }
  }
}
