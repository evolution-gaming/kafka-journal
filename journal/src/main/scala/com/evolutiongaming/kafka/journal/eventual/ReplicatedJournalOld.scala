package com.evolutiongaming.kafka.journal.eventual

import java.time.Instant

import cats.data.{NonEmptyList => Nel, NonEmptyMap => Nem}
import cats.effect.Resource
import cats.implicits._
import cats.{Applicative, FlatMap, Monad, ~>}
import com.evolutiongaming.catshelper.{ApplicativeThrowable, BracketThrowable, Log}
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.skafka.{Offset, Partition, Topic}
import com.evolutiongaming.smetrics.MetricsHelper._
import com.evolutiongaming.smetrics._

import scala.concurrent.duration.FiniteDuration


trait ReplicatedJournalOld[F[_]] {

  def topics: F[Iterable[Topic]]

  def pointers(topic: Topic): F[TopicPointers]

  def append(
    key: Key,
    partitionOffset: PartitionOffset,
    timestamp: Instant,
    expireAfter: Option[FiniteDuration],
    events: Nel[EventRecord]
  ): F[Unit]

  def delete(
    key: Key,
    partitionOffset: PartitionOffset,
    timestamp: Instant,
    deleteTo: SeqNr,
    origin: Option[Origin]
  ): F[Unit]

  def save(topic: Topic, pointers: Nem[Partition, Offset], timestamp: Instant): F[Unit]
}

object ReplicatedJournalOld {

  def apply[F[_]](implicit F: ReplicatedJournalOld[F]): ReplicatedJournalOld[F] = F


  def apply[F[_] : BracketThrowable](replicatedJournal: ReplicatedJournal2[F]): ReplicatedJournalOld[F] = {
    new ReplicatedJournalOld[F] {

      def topics = replicatedJournal.topics

      def pointers(topic: Topic) = {
        replicatedJournal
          .journal(topic)
          .use { _.pointers }
      }

      def append(
        key: Key,
        partitionOffset: PartitionOffset,
        timestamp: Instant,
        expireAfter: Option[FiniteDuration],
        events: Nel[EventRecord]
      ) = {
        replicatedJournal
          .journal(key.topic)
          .use { journal =>
            journal
              .journal(key.id)
              .use { _.append(partitionOffset, timestamp, expireAfter, events) }
          }
      }

      def delete(
        key: Key,
        partitionOffset: PartitionOffset,
        timestamp: Instant,
        deleteTo: SeqNr,
        origin: Option[Origin]
      ) = {
        replicatedJournal
          .journal(key.topic)
          .use { journal =>
            journal
              .journal(key.id)
              .use { _.delete(partitionOffset, timestamp, deleteTo, origin) }
          }
      }

      def save(topic: Topic, pointers: Nem[Partition, Offset], timestamp: Instant) = {
        replicatedJournal
          .journal(topic)
          .use { _.save(pointers, timestamp) }
      }
    }
  }


  def apply[F[_] : FlatMap : MeasureDuration](journal: ReplicatedJournalOld[F], log: Log[F]): ReplicatedJournalOld[F] = {

    implicit val log1 = log

    new ReplicatedJournalOld[F] {

      def topics = {
        for {
          d <- MeasureDuration[F].start
          r <- journal.topics
          d <- d
          _ <- Log[F].debug(s"topics in ${ d.toMillis }ms, r: ${ r.mkString(",") }")
        } yield r
      }

      def pointers(topic: Topic) = {
        for {
          d <- MeasureDuration[F].start
          r <- journal.pointers(topic)
          d <- d
          _ <- Log[F].debug(s"$topic pointers in ${ d.toMillis }ms, result: $r")
        } yield r
      }

      def append(
        key: Key,
        partitionOffset: PartitionOffset,
        timestamp: Instant,
        expireAfter: Option[FiniteDuration],
        events: Nel[EventRecord]
      ) = {
        for {
          d <- MeasureDuration[F].start
          r <- journal.append(key, partitionOffset, timestamp, expireAfter, events)
          d <- d
          _ <- Log[F].debug {
            val origin = events.head.origin
            val originStr = origin.fold("") { origin => s", origin: $origin" }
            val expireAfterStr = expireAfter.fold("") { expireAfter => s", expireAfter: $expireAfter" }
            s"$key append in ${ d.toMillis }ms, " +
              s"offset: $partitionOffset$originStr$expireAfterStr, " +
              s"events: ${ events.toList.mkString(",") }"
          }
        } yield r
      }

      def delete(
        key: Key,
        partitionOffset: PartitionOffset,
        timestamp: Instant,
        deleteTo: SeqNr,
        origin: Option[Origin]
      ) = {
        for {
          d <- MeasureDuration[F].start
          r <- journal.delete(key, partitionOffset, timestamp, deleteTo, origin)
          d <- d
          _ <- Log[F].debug {
            val originStr = origin.fold("") { origin => s", origin: $origin" }
            s"$key delete in ${ d.toMillis }ms, offset: $partitionOffset, deleteTo: $deleteTo$originStr"
          }
        } yield r
      }

      def save(topic: Topic, pointers: Nem[Partition, Offset], timestamp: Instant) = {
        for {
          d <- MeasureDuration[F].start
          r <- journal.save(topic, pointers, timestamp)
          d <- d
          _ <- Log[F].debug(s"$topic save in ${ d.toMillis }ms, pointers: ${pointers.mkString_(",")}, timestamp: $timestamp")
        } yield r
      }
    }
  }


  def apply[F[_] : FlatMap : MeasureDuration](journal: ReplicatedJournalOld[F], metrics: Metrics[F]): ReplicatedJournalOld[F] = {

    new ReplicatedJournalOld[F] {

      def topics = {
        for {
          d <- MeasureDuration[F].start
          r <- journal.topics
          d <- d
          _ <- metrics.topics(d)
        } yield r
      }

      def pointers(topic: Topic) = {
        for {
          d <- MeasureDuration[F].start
          r <- journal.pointers(topic)
          d <- d
          _ <- metrics.pointers(d)
        } yield r
      }

      def append(
        key: Key,
        partitionOffset: PartitionOffset,
        timestamp: Instant,
        expireAfter: Option[FiniteDuration],
        events: Nel[EventRecord]
      ) = {
        for {
          d <- MeasureDuration[F].start
          r <- journal.append(key, partitionOffset, timestamp, expireAfter, events)
          d <- d
          _ <- metrics.append(topic = key.topic, latency = d, events = events.size)
        } yield r
      }

      def delete(
        key: Key,
        partitionOffset: PartitionOffset,
        timestamp: Instant,
        deleteTo: SeqNr,
        origin: Option[Origin]
      ) = {
        for {
          d <- MeasureDuration[F].start
          r <- journal.delete(key, partitionOffset, timestamp, deleteTo, origin)
          d <- d
          _ <- metrics.delete(key.topic, d)
        } yield r
      }

      def save(topic: Topic, pointers: Nem[Partition, Offset], timestamp: Instant) = {
        for {
          d <- MeasureDuration[F].start
          r <- journal.save(topic, pointers, timestamp)
          d <- d
          _ <- metrics.save(topic, d)
        } yield r
      }
    }
  }


  def empty[F[_] : Applicative]: ReplicatedJournalOld[F] = new ReplicatedJournalOld[F] {

    def topics = Iterable.empty[Topic].pure[F]

    def pointers(topic: Topic) = TopicPointers.empty.pure[F]

    def append(
      key: Key,
      partitionOffset: PartitionOffset,
      timestamp: Instant,
      expireAfter: Option[FiniteDuration],
      events: Nel[EventRecord]
    ) = ().pure[F]

    def delete(
      key: Key,
      partitionOffset: PartitionOffset,
      timestamp: Instant,
      deleteTo: SeqNr,
      origin: Option[Origin]
    ) = ().pure[F]

    def save(topic: Topic, pointers: Nem[Partition, Offset], timestamp: Instant) = ().pure[F]
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
    ): Resource[F, ReplicatedJournalOld.Metrics[F]] = {

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

        new ReplicatedJournalOld.Metrics[F] {

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


  implicit class ReplicatedJournalOps[F[_]](val self: ReplicatedJournalOld[F]) extends AnyVal {

    def mapK[G[_]](f: F ~> G): ReplicatedJournalOld[G] = new ReplicatedJournalOld[G] {

      def topics = f(self.topics)

      def pointers(topic: Topic) = f(self.pointers(topic))

      def append(
        key: Key,
        partitionOffset: PartitionOffset,
        timestamp: Instant,
        expireAfter: Option[FiniteDuration],
        events: Nel[EventRecord]
      ) = {
        f(self.append(key, partitionOffset, timestamp, expireAfter, events))
      }

      def delete(
        key: Key,
        partitionOffset: PartitionOffset,
        timestamp: Instant,
        deleteTo: SeqNr,
        origin: Option[Origin]
      ) = {
        f(self.delete(key, partitionOffset, timestamp, deleteTo, origin))
      }

      def save(topic: Topic, pointers: Nem[Partition, Offset], timestamp: Instant) = {
        f(self.save(topic, pointers, timestamp))
      }
    }


    def withLog(log: Log[F])(implicit flatMap: FlatMap[F], measureDuration: MeasureDuration[F]): ReplicatedJournalOld[F] = {
      ReplicatedJournalOld[F](self, log)
    }


    def withMetrics(metrics: Metrics[F])(implicit flatMap: FlatMap[F], measureDuration: MeasureDuration[F]): ReplicatedJournalOld[F] = {
      ReplicatedJournalOld(self, metrics)
    }


    def enhanceError(implicit F: ApplicativeThrowable[F]): ReplicatedJournalOld[F] = {

      def error[A](msg: String, cause: Throwable) = {
        JournalError(s"ReplicatedJournal.$msg failed with $cause", cause.some).raiseError[F, A]
      }

      new ReplicatedJournalOld[F] {

        def topics = {
          self
            .topics
            .handleErrorWith { a => error(s"topics", a) }
        }

        def pointers(topic: Topic) = {
          self
            .pointers(topic)
            .handleErrorWith { a => error(s"pointers topic: $topic", a) }
        }

        def append(
          key: Key,
          partitionOffset: PartitionOffset,
          timestamp: Instant,
          expireAfter: Option[FiniteDuration],
          events: Nel[EventRecord]
        ) = {
          self
            .append(key, partitionOffset, timestamp, expireAfter, events)
            .handleErrorWith { a =>
              error(s"append " +
                s"key: $key, " +
                s"offset: $partitionOffset, " +
                s"timestamp: $timestamp, " +
                s"expireAfter: $expireAfter, " +
                s"events: $events", a)
            }
        }

        def delete(
          key: Key,
          partitionOffset: PartitionOffset,
          timestamp: Instant,
          deleteTo: SeqNr,
          origin: Option[Origin]
        ) = {
          self
            .delete(key, partitionOffset, timestamp, deleteTo, origin)
            .handleErrorWith { a =>
              error(
                s"delete " +
                  s"key: $key, " +
                  s"offset: $partitionOffset, " +
                  s"timestamp: $timestamp, " +
                  s"deleteTo: $deleteTo, " +
                  s"origin: $origin", a)
            }
        }

        def save(topic: Topic, pointers: Nem[Partition, Offset], timestamp: Instant) = {
          self
            .save(topic, pointers, timestamp)
            .handleErrorWith { a =>
              error(s"save " +
                s"topic: $topic, " +
                s"pointers: $pointers, " +
                s"timestamp: $timestamp", a)
            }
        }
      }
    }
  }
}