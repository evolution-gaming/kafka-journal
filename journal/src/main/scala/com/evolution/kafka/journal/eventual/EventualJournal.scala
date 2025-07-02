package com.evolution.kafka.journal.eventual

import cats.*
import cats.arrow.FunctionK
import cats.effect.Resource
import cats.effect.syntax.all.*
import cats.syntax.all.*
import com.evolutiongaming.catshelper.{Log, MeasureDuration, MonadThrowable}
import com.evolution.kafka.journal.*
import com.evolution.kafka.journal.util.StreamHelper.*
import com.evolutiongaming.skafka.{Offset, Partition, Topic}
import com.evolutiongaming.smetrics.*
import com.evolutiongaming.smetrics.MetricsHelper.*
import com.evolutiongaming.sstream.Stream

import scala.concurrent.duration.FiniteDuration

/**
 * Reader of the replicated journal (Cassandra by default).
 *
 * This allows to access the data replicated from Kafka to Cassandra. Note, that all the methods of
 * this class have eventual nature, i.e. there could be newer events already in Kafka, but not yet
 * replicated to Cassandra.
 *
 * There could be other non-Cassandra implementations of this interface.
 */
trait EventualJournal[F[_]] {

  /**
   * Gets a last replicated [[PartitionOffset]] and [[SeqNr]] for specific journal [[Key]].
   *
   * @param key
   *   Unique identifier of a journal including a topic where it is stored in Kafka.
   * @return
   *   [[JournalPointer]] containing a partition, an offset and sequence number of a last replicated
   *   event.
   */
  def pointer(key: Key): F[Option[JournalPointer]]

  /**
   * Gets the last replicated offset for a partition topic.
   *
   * @param topic
   *   Kafka topic.
   * @param partition
   *   Topic partition to get offset for.
   * @return
   *   Partition offset, or `None` if such topic/partition pair never replicated.
   */
  def offset(topic: Topic, partition: Partition): F[Option[Offset]]

  /**
   * Reads the replicated event journal.
   *
   * Streams all events already replicated to a storage (Cassandra by default). It does not keep
   * streaming forever and ends the stream when the last replicated event is read.
   *
   * While it is possible to keep streaming forever by polling this method from time to time, the
   * recommended way is to read Kafka instead.
   *
   * @param key
   *   Unique identifier of a journal including a topic where it is stored in Kafka.
   * @param from
   *   First [[SeqNr]] to read.
   * @return
   *   Stream of events found replicated to Cassandra ordered by [[SeqNr]].
   */
  def read(key: Key, from: SeqNr): Stream[F, EventRecord[EventualPayloadAndType]]

  /**
   * Streams ids of journals replicated from a given topic.
   *
   * Streams all ids already replicated to a storage (Cassandra by default). It does not keep
   * streaming forever and ends the stream when the last replicated journal is found.
   *
   * @param topic
   *   Kafka topic name where the journals are being stored in.
   * @return
   *   Stream of ids found replicated to Cassandra.
   */
  def ids(topic: Topic): Stream[F, String]
}

object EventualJournal {

  def apply[F[_]](
    implicit
    F: EventualJournal[F],
  ): EventualJournal[F] = F

  private sealed abstract class Empty

  def empty[F[_]: Applicative]: EventualJournal[F] = new Empty with EventualJournal[F] {

    def read(key: Key, from: SeqNr) = Stream.empty[F, EventRecord[EventualPayloadAndType]]

    def pointer(key: Key) = none[JournalPointer].pure[F]

    def ids(topic: Topic) = Stream.empty[F, String]

    def offset(topic: Topic, partition: Partition): F[Option[Offset]] = none[Offset].pure[F]
  }

  trait Metrics[F[_]] {

    def offset(topic: Topic, latency: FiniteDuration): F[Unit]

    def read(topic: Topic, latency: FiniteDuration): F[Unit]

    def read(topic: Topic): F[Unit]

    def pointer(topic: Topic, latency: FiniteDuration): F[Unit]

    def ids(topic: Topic, latency: FiniteDuration): F[Unit]
  }

  object Metrics {

    def empty[F[_]: Applicative]: Metrics[F] = const(Applicative[F].unit)

    private sealed abstract class Const

    def const[F[_]](unit: F[Unit]): Metrics[F] = new Const with Metrics[F] {

      def read(topic: Topic, latency: FiniteDuration) = unit

      def read(topic: Topic) = unit

      def pointer(topic: Topic, latency: FiniteDuration) = unit

      def ids(topic: Topic, latency: FiniteDuration) = unit

      def offset(topic: Topic, latency: FiniteDuration): F[Unit] = unit
    }

    private sealed abstract class Main

    def make[F[_]](
      registry: CollectorRegistry[F],
      prefix: String = "eventual_journal",
    ): Resource[F, Metrics[F]] = {

      val versionGauge = registry.gauge(
        name = s"${ prefix }_info",
        help = "Journal version information",
        labels = LabelNames("version"),
      )

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
        labels = LabelNames("topic"),
      )

      for {
        versionGauge <- versionGauge
        _ <- versionGauge.labels(Version.current.value).set(1).toResource
        latencySummary <- latencySummary
        eventsSummary <- eventsSummary
      } yield {

        def observeLatency(name: String, topic: Topic, latency: FiniteDuration) = {
          latencySummary
            .labels(topic, name)
            .observe(latency.toNanos.nanosToSeconds)
        }

        new Main with Metrics[F] {

          def read(topic: Topic, latency: FiniteDuration) = {
            observeLatency(name = "read", topic = topic, latency = latency)
          }

          def read(topic: Topic) = {
            eventsSummary.labels(topic).observe(1.0)
          }

          def pointer(topic: Topic, latency: FiniteDuration) = {
            observeLatency(name = "pointer", topic = topic, latency = latency)
          }

          def ids(topic: Topic, latency: FiniteDuration) = {
            observeLatency(name = "ids", topic = topic, latency = latency)
          }

          def offset(topic: Topic, latency: FiniteDuration): F[Unit] = {
            observeLatency(name = "offset", topic = topic, latency = latency)
          }
        }
      }
    }
  }

  private sealed abstract class MapK

  private sealed abstract class WithMetrics

  private sealed abstract class WithLog

  private sealed abstract class EnhanceError

  implicit class EventualJournalOps[F[_]](val self: EventualJournal[F]) extends AnyVal {

    def mapK[G[_]](fg: F ~> G, gf: G ~> F): EventualJournal[G] = new MapK with EventualJournal[G] {

      def read(key: Key, from1: SeqNr) = self.read(key, from1).mapK(fg, gf)

      def pointer(key: Key) = fg(self.pointer(key))

      def ids(topic: Topic) = self.ids(topic).mapK(fg, gf)

      def offset(topic: Topic, partition: Partition): G[Option[Offset]] = fg(self.offset(topic, partition))
    }

    def withLog(
      log: Log[F],
    )(implicit
      F: FlatMap[F],
      measureDuration: MeasureDuration[F],
    ): EventualJournal[F] = {

      val functionKId = FunctionK.id[F]

      new WithLog with EventualJournal[F] {

        def read(key: Key, from: SeqNr) = {
          val logging = new (F ~> F) {
            def apply[A](fa: F[A]) = {
              for {
                d <- MeasureDuration[F].start
                r <- fa
                d <- d
                _ <- log.debug(s"$key read in ${ d.toMillis }ms, from: $from, result: $r")
              } yield r
            }
          }
          self.read(key, from).mapK(logging, functionKId)
        }

        def pointer(key: Key) = {
          for {
            d <- MeasureDuration[F].start
            r <- self.pointer(key)
            d <- d
            _ <- log.debug(s"$key pointer in ${ d.toMillis }ms, result: $r")
          } yield r
        }

        def ids(topic: Topic) = {
          val logging = new (F ~> F) {
            def apply[A](fa: F[A]) = {
              for {
                d <- MeasureDuration[F].start
                r <- fa
                d <- d
                _ <- log.debug(s"$topic ids in ${ d.toMillis }ms, result: $r")
              } yield r
            }
          }
          self.ids(topic).mapK(logging, functionKId)
        }

        def offset(topic: Topic, partition: Partition): F[Option[Offset]] = {
          for {
            d <- MeasureDuration[F].start
            r <- self.offset(topic, partition)
            d <- d
            _ <- log.debug(s"$topic $partition offset in ${ d.toMillis }ms, result: $r")
          } yield r
        }

      }
    }

    def withMetrics(
      metrics: Metrics[F],
    )(implicit
      F: FlatMap[F],
      measureDuration: MeasureDuration[F],
    ): EventualJournal[F] = {

      val functionKId = FunctionK.id[F]

      new WithMetrics with EventualJournal[F] {

        def read(key: Key, from: SeqNr) = {
          val measure = new (F ~> F) {
            def apply[A](fa: F[A]) = {
              for {
                d <- MeasureDuration[F].start
                r <- fa
                d <- d
                _ <- metrics.read(topic = key.topic, latency = d)
              } yield r
            }
          }

          for {
            a <- self.read(key, from).mapK(measure, functionKId)
            _ <- metrics.read(key.topic).toStream
          } yield a
        }

        def pointer(key: Key) = {
          for {
            d <- MeasureDuration[F].start
            r <- self.pointer(key)
            d <- d
            _ <- metrics.pointer(key.topic, d)
          } yield r
        }

        def ids(topic: Topic) = {
          val measure = new (F ~> F) {
            def apply[A](fa: F[A]) = {
              for {
                d <- MeasureDuration[F].start
                r <- fa
                d <- d
                _ <- metrics.ids(topic, d)
              } yield r
            }
          }
          self.ids(topic).mapK(measure, functionKId)
        }

        def offset(topic: Topic, partition: Partition): F[Option[Offset]] = {
          for {
            d <- MeasureDuration[F].start
            r <- self.offset(topic, partition)
            d <- d
            _ <- metrics.offset(topic, d)
          } yield r
        }

      }
    }

    def enhanceError(
      implicit
      F: MonadThrowable[F],
    ): EventualJournal[F] = {

      def error[A](msg: String, cause: Throwable) = {
        JournalError(s"EventualJournal.$msg failed with $cause", cause).raiseError[F, A]
      }

      new EnhanceError with EventualJournal[F] {

        def read(key: Key, from: SeqNr) = {
          self
            .read(key, from)
            .handleErrorWith { (a: Throwable) =>
              error[EventRecord[EventualPayloadAndType]](s"read key: $key, from: $from", a).toStream
            }
        }

        def pointer(key: Key) = {
          self
            .pointer(key)
            .handleErrorWith { a => error(s"pointer key: $key", a) }
        }

        def ids(topic: Topic) = {
          self
            .ids(topic)
            .handleErrorWith { a => error[String](s"ids topic: $topic", a).toStream }
        }

        def offset(topic: Topic, partition: Partition): F[Option[Offset]] = {
          self
            .offset(topic, partition)
            .handleErrorWith { a => error(s"offset topic: $topic, partition $partition", a) }
        }
      }
    }
  }
}
