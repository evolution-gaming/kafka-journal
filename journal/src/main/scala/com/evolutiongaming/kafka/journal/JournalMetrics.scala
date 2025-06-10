package com.evolutiongaming.kafka.journal

import cats.*
import cats.effect.*
import cats.syntax.all.*
import com.evolutiongaming.skafka.Topic
import com.evolutiongaming.smetrics.*
import com.evolutiongaming.smetrics.MetricsHelper.*

import scala.concurrent.duration.*

trait JournalMetrics[F[_]] {

  def append(topic: Topic, latency: FiniteDuration, events: Int): F[Unit]

  def read(topic: Topic, latency: FiniteDuration): F[Unit]

  def read(topic: Topic): F[Unit]

  def pointer(topic: Topic, latency: FiniteDuration): F[Unit]

  def delete(topic: Topic, latency: FiniteDuration): F[Unit]

  def purge(topic: Topic, latency: FiniteDuration): F[Unit]

  def failure(topic: Topic, name: String): F[Unit]
}

object JournalMetrics {

  def empty[F[_]: Applicative]: JournalMetrics[F] = const(().pure[F])

  def const[F[_]](unit: F[Unit]): JournalMetrics[F] = {
    class Const
    new Const with JournalMetrics[F] {

      def append(topic: Topic, latency: FiniteDuration, events: Int) = unit

      def read(topic: Topic, latency: FiniteDuration) = unit

      def read(topic: Topic) = unit

      def pointer(topic: Topic, latency: FiniteDuration) = unit

      def delete(topic: Topic, latency: FiniteDuration) = unit

      def purge(topic: Topic, latency: FiniteDuration) = unit

      def failure(topic: Topic, name: String) = unit
    }
  }

  def make[F[_]: Monad](
    registry: CollectorRegistry[F],
    prefix: String = "journal",
  ): Resource[F, JournalMetrics[F]] = {

    val latencySummary = registry.summary(
      name = s"${ prefix }_topic_latency",
      help = "Journal call latency in seconds",
      quantiles = Quantiles.Default,
      labels = LabelNames("topic", "type"),
    )

    val eventsSummary = registry.summary(
      name = s"${ prefix }_events",
      help = "Number of events",
      quantiles = Quantiles.Empty,
      labels = LabelNames("topic", "type"),
    )

    val resultCounter = registry.counter(
      name = s"${ prefix }_results",
      help = "Call result: success or failure",
      labels = LabelNames("topic", "type", "result"),
    )

    for {
      latencySummary <- latencySummary
      eventsSummary <- eventsSummary
      resultCounter <- resultCounter
    } yield {

      def observeLatency(name: String, topic: Topic, latency: FiniteDuration) = {
        for {
          _ <- latencySummary.labels(topic, name).observe(latency.toNanos.nanosToSeconds)
          _ <- resultCounter.labels(topic, name, "success").inc()
        } yield {}
      }

      def observeEvents(name: String, topic: Topic, events: Int) = {
        eventsSummary.labels(topic, name).observe(events.toDouble)
      }

      class Main
      new Main with JournalMetrics[F] {

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

        def purge(topic: Topic, latency: FiniteDuration) = {
          observeLatency(name = "purge", topic = topic, latency = latency)
        }

        def failure(topic: Topic, name: String) = {
          resultCounter.labels(topic, name, "failure").inc()
        }
      }
    }
  }
}
