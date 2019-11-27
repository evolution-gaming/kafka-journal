package com.evolutiongaming.kafka.journal.eventual



import cats.effect.Resource
import cats.implicits._
import cats.{Applicative, Defer, Monad, ~>}
import com.evolutiongaming.catshelper.{ApplicativeThrowable, BracketThrowable, Log}
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.skafka.Topic
import com.evolutiongaming.smetrics.MetricsHelper._
import com.evolutiongaming.smetrics._

import scala.concurrent.duration.FiniteDuration

trait ReplicatedJournal[F[_]] {

  def topics: F[List[Topic]]

  def journal(topic: Topic): Resource[F, ReplicatedTopicJournal[F]]
}

object ReplicatedJournal {

  def empty[F[_] : Applicative]: ReplicatedJournal[F] = new ReplicatedJournal[F] {

    def topics = List.empty[Topic].pure[F]

    def journal(topic: Topic) = {
      Resource.liftF(ReplicatedTopicJournal.empty[F].pure[F])
    }
  }


  def apply[F[_] : Applicative](replicatedJournal: ReplicatedJournalFlat[F]): ReplicatedJournal[F] = {

    new ReplicatedJournal[F] {

      def topics = replicatedJournal.topics

      def journal(topic: Topic) = {
        val replicatedTopicJournal = ReplicatedTopicJournal(topic, replicatedJournal)
        Resource.liftF(replicatedTopicJournal.pure[F])
      }
    }
  }


  implicit class ReplicatedJournalOps[F[_]](val self: ReplicatedJournal[F]) extends AnyVal {

    def mapK[G[_]](
      f: F ~> G)(implicit
      B: BracketThrowable[F],
      D: Defer[G],
      G: Applicative[G]
    ): ReplicatedJournal[G] = new ReplicatedJournal[G] {

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
    ): ReplicatedJournal[F] = {

      new ReplicatedJournal[F] {

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
    }


    def withMetrics(
      metrics: Metrics[F])(implicit
      F: Monad[F],
      measureDuration: MeasureDuration[F]
    ): ReplicatedJournal[F] = {
      new ReplicatedJournal[F] {

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
    }


    def enhanceError(implicit F: ApplicativeThrowable[F]): ReplicatedJournal[F] = {

      def error[A](msg: String, cause: Throwable) = {
        JournalError(s"ReplicatedJournal.$msg failed with $cause", cause.some).raiseError[F, A]
      }

      new ReplicatedJournal[F] {

        def topics = {
          self
            .topics
            .handleErrorWith { a => error(s"topics", a) }
        }

        def journal(topic: Topic) = {
          self
            .journal(topic)
            .map { _.enhanceError(topic) }
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