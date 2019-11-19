package com.evolutiongaming.kafka.journal.eventual

import cats.effect.Resource
import cats.implicits._
import cats.{Applicative, Monad}
import com.evolutiongaming.skafka.Topic
import com.evolutiongaming.smetrics.MetricsHelper._
import com.evolutiongaming.smetrics._

import scala.concurrent.duration.FiniteDuration

trait ReplicatedJournal[F[_]] {

  def topics: F[Iterable[Topic]]

  def journal(topic: Topic): Resource[F, ReplicatedTopicJournal[F]]
}

object ReplicatedJournal {

  def apply[F[_] : Applicative](replicatedJournal: ReplicatedJournalOld[F]): ReplicatedJournal[F] = {

    new ReplicatedJournal[F] {

      def topics = replicatedJournal.topics

      def journal(topic: Topic) = {
        val replicatedTopicJournal = ReplicatedTopicJournal(topic, replicatedJournal)
        Resource.liftF(replicatedTopicJournal.pure[F])
      }
    }
  }


  trait Metrics[F[_]] {

    def topics(latency: FiniteDuration): F[Unit]

    def pointers(latency: FiniteDuration): F[Unit]

    def append(topic: Topic, latency: FiniteDuration, events: Int): F[Unit]

    def delete(topic: Topic, latency: FiniteDuration): F[Unit]

    def save(topic: Topic, latency: FiniteDuration): F[Unit]
  }

  object Metrics {

    def empty[F[_] : Applicative]: Metrics[F] = const(().pure[F])


    def const[F[_]](unit: F[Unit]): Metrics[F] = new Metrics[F] {

      def topics(latency: FiniteDuration) = unit

      def pointers(latency: FiniteDuration) = unit

      def append(topic: Topic, latency: FiniteDuration, events: Int) = unit

      def delete(topic: Topic, latency: FiniteDuration) = unit

      def save(topic: Topic, latency: FiniteDuration) = unit
    }


    def of[F[_] : Monad](
      registry: CollectorRegistry[F],
      prefix: String = "replicated_journal"
    ): Resource[F, Metrics[F]] = {

      val quantiles = Quantiles(
        Quantile(0.9, 0.05),
        Quantile(0.99, 0.005))

      val latencySummary = registry.summary(
        name      = s"${ prefix }_latency",
        help      = "Journal call latency in seconds",
        quantiles = quantiles,
        labels    = LabelNames("type"))

      val topicLatencySummary = registry.summary(
        name      = s"${ prefix }_topic_latency",
        help      = "Journal topic call latency in seconds",
        quantiles = quantiles,
        labels    = LabelNames("topic", "type"))

      val eventsSummary = registry.summary(
        name      = s"${ prefix }_events",
        help      = "Number of events saved",
        quantiles = Quantiles.Empty,
        labels    = LabelNames("topic"))

      for {
        latencySummary      <- latencySummary
        topicLatencySummary <- topicLatencySummary
        eventsSummary       <- eventsSummary
      } yield {

        def observeTopicLatency(name: String, topic: Topic, latency: FiniteDuration) = {
          topicLatencySummary
            .labels(topic, name)
            .observe(latency.toNanos.nanosToSeconds)
        }

        def observeLatency(name: String, latency: FiniteDuration) = {
          latencySummary
            .labels(name)
            .observe(latency.toNanos.nanosToSeconds)
        }

        new Metrics[F] {

          def topics(latency: FiniteDuration) = {
            observeLatency(name = "topics", latency = latency)
          }

          def pointers(latency: FiniteDuration) = {
            observeLatency(name = "pointers", latency = latency)
          }

          def append(topic: Topic, latency: FiniteDuration, events: Int) = {
            for {
              _ <- eventsSummary.labels(topic).observe(events.toDouble)
              _ <- observeTopicLatency(name = "append", topic = topic, latency = latency)
            } yield {}
          }

          def delete(topic: Topic, latency: FiniteDuration) = {
            observeTopicLatency(name = "delete", topic = topic, latency = latency)
          }

          def save(topic: Topic, latency: FiniteDuration) = {
            observeTopicLatency(name = "save", topic = topic, latency = latency)
          }
        }
      }
    }
  }
}