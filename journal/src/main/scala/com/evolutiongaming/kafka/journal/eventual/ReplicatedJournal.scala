package com.evolutiongaming.kafka.journal.eventual

import cats.effect.Resource
import cats.effect.syntax.all.*
import cats.syntax.all.*
import cats.{Applicative, Monad, ~>}
import com.evolutiongaming.catshelper.{BracketThrowable, Log, MeasureDuration, MonadThrowable}
import com.evolutiongaming.kafka.journal.*
import com.evolutiongaming.skafka.Topic
import com.evolutiongaming.smetrics.*
import com.evolutiongaming.smetrics.MetricsHelper.*

import scala.collection.immutable.SortedSet
import scala.concurrent.duration.FiniteDuration

/** Write-only implementation of a journal stored to eventual storage, i.e.
  * Cassandra.
  *
  * This class is used to replicate events read from Kafka, hence the name.
  *
  * It follows a hierarchical structure, i.e., roughly speaking, it is
  * a factory of topic-specific [[ReplicatedTopicJournal]] instances.
  * 
  * @see [[EventualJournal]] for a read-only counterpart of this class.
  */
trait ReplicatedJournal[F[_]] {

  /** Select list of kafka topics previously saved into eventual storage */
  def topics: F[SortedSet[Topic]]

  /** Create a topic-specific write-only journal */
  def journal(topic: Topic): Resource[F, ReplicatedTopicJournal[F]]
}

object ReplicatedJournal {

  private sealed abstract class Empty

  def empty[F[_]: Applicative]: ReplicatedJournal[F] = new Empty with ReplicatedJournal[F] {

    def topics: F[SortedSet[Topic]] = SortedSet.empty[Topic].pure[F]

    def journal(topic: Topic): Resource[F, ReplicatedTopicJournal[F]] = {
      ReplicatedTopicJournal
        .empty[F]
        .pure[F]
        .toResource
    }
  }

  private sealed abstract class MapK

  private sealed abstract class WithLog

  private sealed abstract class WithMetrics

  private sealed abstract class WithEnhanceError

  implicit class ReplicatedJournalOps[F[_]](val self: ReplicatedJournal[F]) extends AnyVal {

    def mapK[G[_]](f: F ~> G)(implicit B: BracketThrowable[F], GT: BracketThrowable[G]): ReplicatedJournal[G] = new MapK
      with ReplicatedJournal[G] {

      def topics: G[SortedSet[Topic]] = f(self.topics)

      def journal(topic: Topic): Resource[G, ReplicatedTopicJournal[G]] = {
        self
          .journal(topic)
          .map { _.mapK(f) }
          .mapK(f)
      }
    }

    def withLog(log: Log[F])(implicit F: Monad[F], measureDuration: MeasureDuration[F]): ReplicatedJournal[F] = new WithLog
      with ReplicatedJournal[F] {

      def topics: F[SortedSet[Topic]] = {
        for {
          d <- MeasureDuration[F].start
          r <- self.topics
          d <- d
          _ <- log.debug(s"topics in ${d.toMillis}ms, r: ${r.mkString(",")}")
        } yield r
      }

      def journal(topic: Topic): Resource[F, ReplicatedTopicJournal[F]] = {
        self
          .journal(topic)
          .map { _.withLog(topic, log) }
      }
    }

    def withMetrics(metrics: Metrics[F])(implicit F: Monad[F], measureDuration: MeasureDuration[F]): ReplicatedJournal[F] =
      new WithMetrics with ReplicatedJournal[F] {

        def topics: F[SortedSet[Topic]] = {
          for {
            d <- MeasureDuration[F].start
            r <- self.topics
            d <- d
            _ <- metrics.topics(d)
          } yield r
        }

        def journal(topic: Topic): Resource[F, ReplicatedTopicJournal[F]] = {
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

        def topics: F[SortedSet[Topic]] = {
          self
            .topics
            .adaptError { case a => journalError(s"topics", a) }
        }

        def journal(topic: Topic): Resource[F, ReplicatedTopicJournal[F]] = {
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

    def offsetsGet(latency: FiniteDuration): F[Unit]

    def offsetsUpdate(topic: Topic, latency: FiniteDuration): F[Unit]

    def offsetsCreate(topic: Topic, latency: FiniteDuration): F[Unit]

    def append(topic: Topic, latency: FiniteDuration, events: Int): F[Unit]

    def delete(topic: Topic, latency: FiniteDuration): F[Unit]

    def purge(topic: Topic, latency: FiniteDuration): F[Unit]
  }

  object Metrics {

    def empty[F[_]: Applicative]: Metrics[F] = const(().pure[F])

    def const[F[_]](unit: F[Unit]): Metrics[F] = {
      class Const
      new Const with Metrics[F] {

        def topics(latency: FiniteDuration): F[Unit] = unit

        def offsetsGet(latency: FiniteDuration): F[Unit] = unit

        def offsetsUpdate(topic: Topic, latency: FiniteDuration): F[Unit] = unit

        def offsetsCreate(topic: Topic, latency: FiniteDuration): F[Unit] = unit

        def append(topic: Topic, latency: FiniteDuration, events: Int): F[Unit] = unit

        def delete(topic: Topic, latency: FiniteDuration): F[Unit] = unit

        def purge(topic: Topic, latency: FiniteDuration): F[Unit] = unit
      }
    }

    def of[F[_]: Monad](
      registry: CollectorRegistry[F],
      prefix: String = "replicated_journal",
    ): Resource[F, Metrics[F]] = {

      val versionGauge = registry.gauge(
        name   = s"${prefix}_info",
        help   = "Journal version information",
        labels = LabelNames("version"),
      )

      val latencySummary = registry.summary(
        name      = s"${prefix}_latency",
        help      = "Journal call latency in seconds",
        quantiles = Quantiles.Default,
        labels    = LabelNames("type"),
      )

      val topicLatencySummary = registry.summary(
        name      = s"${prefix}_topic_latency",
        help      = "Journal topic call latency in seconds",
        quantiles = Quantiles.Default,
        labels    = LabelNames("topic", "type"),
      )

      val eventsSummary = registry.summary(
        name      = s"${prefix}_events",
        help      = "Number of events saved",
        quantiles = Quantiles.Empty,
        labels    = LabelNames("topic"),
      )

      for {
        versionGauge                 <- versionGauge
        _                            <- versionGauge.labels(Version.current.value).set(1).toResource
        latencySummary               <- latencySummary
        topicLatencySummary          <- topicLatencySummary
        eventsSummary                <- eventsSummary
      } yield {

        def observeTopicLatency(name: String, topic: Topic, latency: FiniteDuration): F[Unit] = {
          topicLatencySummary
            .labels(topic, name)
            .observe(latency.toNanos.nanosToSeconds)
        }

        def observeLatency(name: String, latency: FiniteDuration): F[Unit] = {
          latencySummary
            .labels(name)
            .observe(latency.toNanos.nanosToSeconds)
        }

        class Main

        new Main with Metrics[F] {

          def topics(latency: FiniteDuration): F[Unit] =
            observeLatency(name = "topics", latency = latency)

          def offsetsGet(latency: FiniteDuration): F[Unit] =
            observeLatency(name = "offsets.get", latency = latency)

          def offsetsUpdate(topic: Topic, latency: FiniteDuration): F[Unit] =
            observeLatency(name = "offsets.update", latency = latency)

          def offsetsCreate(topic: Topic, latency: FiniteDuration): F[Unit] =
            observeLatency(name = "offsets.create", latency = latency)

          def append(topic: Topic, latency: FiniteDuration, events: Int): F[Unit] =
            for {
              _ <- eventsSummary.labels(topic).observe(events.toDouble)
              _ <- observeTopicLatency(name = "append", topic = topic, latency = latency)
            } yield {}

          def delete(topic: Topic, latency: FiniteDuration): F[Unit] =
            observeTopicLatency(name = "delete", topic = topic, latency = latency)

          def purge(topic: Topic, latency: FiniteDuration): F[Unit] =
            observeTopicLatency(name = "purge", topic = topic, latency = latency)

        }
      }
    }
  }
}
