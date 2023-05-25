package com.evolutiongaming.kafka.journal.eventual


import cats.effect.Resource
import cats.effect.syntax.all._
import cats.syntax.all._
import cats.{Applicative, Monad, ~>}
import com.evolutiongaming.catshelper.{BracketThrowable, Log, MeasureDuration, MonadThrowable}
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.skafka.Topic
import com.evolutiongaming.smetrics.MetricsHelper._
import com.evolutiongaming.smetrics._

import scala.collection.immutable.SortedSet
import scala.concurrent.duration.FiniteDuration

trait ReplicatedJournal[F[_]] {

  def topics: F[SortedSet[Topic]]

  def journal(topic: Topic): Resource[F, ReplicatedTopicJournal[F]]
}

object ReplicatedJournal {

  private sealed abstract class Empty

  def empty[F[_] : Applicative]: ReplicatedJournal[F] = new Empty with ReplicatedJournal[F] {

    def topics = SortedSet.empty[Topic].pure[F]

    def journal(topic: Topic) = {
      ReplicatedTopicJournal
        .empty[F]
        .pure[F]
        .toResource
    }
  }

  @deprecated("use `fromFlat` instead", "2021-05-25")
  def apply[F[_]: Applicative](replicatedJournal: ReplicatedJournalFlat[F]): ReplicatedJournal[F] = {
    fromFlat(replicatedJournal)
  }

  private sealed abstract class FromFlat

  def fromFlat[F[_]: Applicative](replicatedJournal: ReplicatedJournalFlat[F]): ReplicatedJournal[F] = {

    new FromFlat with ReplicatedJournal[F] {

      def topics = replicatedJournal.topics

      def journal(topic: Topic) = {
        val replicatedTopicJournal = ReplicatedTopicJournal(topic, replicatedJournal)
        replicatedTopicJournal.pure[F].toResource
      }
    }
  }

  private sealed abstract class MapK

  private sealed abstract class WithLog

  private sealed abstract class WithMetrics

  private sealed abstract class WithEnhanceError


  implicit class ReplicatedJournalOps[F[_]](val self: ReplicatedJournal[F]) extends AnyVal {

    def mapK[G[_]](
      f: F ~> G)(implicit
      B: BracketThrowable[F],
      GT: BracketThrowable[G]
    ): ReplicatedJournal[G] = new MapK with ReplicatedJournal[G] {

      def topics = f(self.topics)

      def journal(topic: Topic) = {
        self
          .journal(topic)
          .map { _.mapK(f) }
          .mapK(f)
      }
    }


    def withLog(
      log: Log[F])(implicit
      F: Monad[F],
      measureDuration: MeasureDuration[F]
    ): ReplicatedJournal[F] = new WithLog with ReplicatedJournal[F] {

      def topics = {
        for {
          d <- MeasureDuration[F].start
          r <- self.topics
          d <- d
          _ <- log.debug(s"topics in ${ d.toMillis }ms, r: ${ r.mkString(",") }")
        } yield r
      }

      def journal(topic: Topic) = {
        self
          .journal(topic)
          .map { _.withLog(topic, log) }
      }
    }


    def withMetrics(
      metrics: Metrics[F])(implicit
      F: Monad[F],
      measureDuration: MeasureDuration[F]
    ): ReplicatedJournal[F] = new WithMetrics with ReplicatedJournal[F] {

      def topics = {
        for {
          d <- MeasureDuration[F].start
          r <- self.topics
          d <- d
          _ <- metrics.topics(d)
        } yield r
      }

      def journal(topic: Topic) = {
        self
          .journal(topic)
          .map { _.withMetrics(topic, metrics) }
      }
    }


    def enhanceError(implicit F: MonadThrowable[F]): ReplicatedJournal[F] = {

      def journalError[A](msg: String, cause: Throwable) = {
        JournalError(s"ReplicatedJournal.$msg failed with $cause", cause)
      }

      new WithEnhanceError with ReplicatedJournal[F] {

        def topics = {
          self
            .topics
            .adaptError { case a => journalError(s"topics", a) }
        }

        def journal(topic: Topic) = {
          self
            .journal(topic)
            .map { _.enhanceError(topic) }
            .adaptError { case a => journalError(s"journal, topic: $topic", a) }
        }
      }
    }

    def toFlat(implicit F: BracketThrowable[F]): ReplicatedJournalFlat[F] = ReplicatedJournalFlat(self)
  }


  trait Metrics[F[_]] {

    def topics(latency: FiniteDuration): F[Unit]

    def pointers(latency: FiniteDuration): F[Unit]

    def append(topic: Topic, latency: FiniteDuration, events: Int): F[Unit]

    def delete(topic: Topic, latency: FiniteDuration): F[Unit]

    def purge(topic: Topic, latency: FiniteDuration): F[Unit]

    def save(topic: Topic, latency: FiniteDuration): F[Unit]
  }

  object Metrics {

    def empty[F[_] : Applicative]: Metrics[F] = const(().pure[F])


    def const[F[_]](unit: F[Unit]): Metrics[F] = new Metrics[F] {

      def topics(latency: FiniteDuration) = unit

      def pointers(latency: FiniteDuration) = unit

      def append(topic: Topic, latency: FiniteDuration, events: Int) = unit

      def delete(topic: Topic, latency: FiniteDuration) = unit

      def purge(topic: Topic, latency: FiniteDuration) = unit

      def save(topic: Topic, latency: FiniteDuration) = unit
    }


    def of[F[_] : Monad](
      registry: CollectorRegistry[F],
      prefix: String = "replicated_journal"
    ): Resource[F, Metrics[F]] = {

      val latencySummary = registry.summary(
        name      = s"${ prefix }_latency",
        help      = "Journal call latency in seconds",
        quantiles = Quantiles.Default,
        labels    = LabelNames("type"))

      val topicLatencySummary = registry.summary(
        name      = s"${ prefix }_topic_latency",
        help      = "Journal topic call latency in seconds",
        quantiles = Quantiles.Default,
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

          def purge(topic: Topic, latency: FiniteDuration) = {
            observeTopicLatency(name = "purge", topic = topic, latency = latency)
          }

          def save(topic: Topic, latency: FiniteDuration) = {
            observeTopicLatency(name = "save", topic = topic, latency = latency)
          }
        }
      }
    }
  }
}