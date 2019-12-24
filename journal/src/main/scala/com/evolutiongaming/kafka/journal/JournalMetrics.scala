package com.evolutiongaming.kafka.journal

import cats._
import cats.arrow.FunctionK
import cats.data.{NonEmptyList => Nel}
import cats.effect._
import cats.implicits._
import com.evolutiongaming.catshelper.ClockHelper._
import com.evolutiongaming.catshelper.{FromTry, Log, LogOf, MonadThrowable}
import com.evolutiongaming.kafka.journal.conversions.{EventsToPayload, PayloadToEvents}
import com.evolutiongaming.kafka.journal.eventual.EventualJournal
import com.evolutiongaming.kafka.journal.util.Fail
import com.evolutiongaming.kafka.journal.util.Fail.implicits._
import com.evolutiongaming.kafka.journal.util.StreamHelper._
import com.evolutiongaming.kafka.journal.util.SkafkaHelper._
import com.evolutiongaming.skafka
import com.evolutiongaming.skafka.consumer.ConsumerConfig
import com.evolutiongaming.skafka.producer.{Acks, ProducerConfig, ProducerRecord}
import com.evolutiongaming.skafka.{Bytes => _, _}
import com.evolutiongaming.smetrics.MetricsHelper._
import com.evolutiongaming.smetrics._
import com.evolutiongaming.sstream.Stream
import play.api.libs.json.JsValue
import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader
import scodec.bits.ByteVector

import scala.concurrent.duration._
import scala.util.Try


trait JournalMetrics[F[_]] {

  // TODO expiry: remove topic from all methods
  def append(topic: Topic, latency: FiniteDuration, events: Int): F[Unit]

  def read(topic: Topic, latency: FiniteDuration): F[Unit]

  def read(topic: Topic): F[Unit]

  def pointer(topic: Topic, latency: FiniteDuration): F[Unit]

  def delete(topic: Topic, latency: FiniteDuration): F[Unit]

  def purge(topic: Topic, latency: FiniteDuration): F[Unit]

  def failure(name: String, topic: Topic): F[Unit]
}

object JournalMetrics {

  def empty[F[_] : Applicative]: JournalMetrics[F] = const(().pure[F])


  def const[F[_]](unit: F[Unit]): JournalMetrics[F] = new JournalMetrics[F] {

    def append(topic: Topic, latency: FiniteDuration, events: Int) = unit

    def read(topic: Topic, latency: FiniteDuration) = unit

    def read(topic: Topic) = unit

    def pointer(topic: Topic, latency: FiniteDuration) = unit

    def delete(topic: Topic, latency: FiniteDuration) = unit

    def purge(topic: Topic, latency: FiniteDuration) = unit

    def failure(name: String, topic: Topic) = unit
  }


  def of[F[_] : Monad](
    registry: CollectorRegistry[F],
    prefix: String = "journal"
  ): Resource[F, JournalMetrics[F]] = {

    val latencySummary = registry.summary(
      name = s"${ prefix }_topic_latency",
      help = "Journal call latency in seconds",
      quantiles = Quantiles(
        Quantile(0.9, 0.05),
        Quantile(0.99, 0.005)),
      labels = LabelNames("topic", "type"))

    val eventsSummary = registry.summary(
      name = s"${ prefix }_events",
      help = "Number of events",
      quantiles = Quantiles.Empty,
      labels = LabelNames("topic", "type"))

    val resultCounter = registry.counter(
      name = s"${ prefix }_results",
      help = "Call result: success or failure",
      labels = LabelNames("topic", "type", "result"))

    for {
      latencySummary <- latencySummary
      eventsSummary  <- eventsSummary
      resultCounter  <- resultCounter
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

      new JournalMetrics[F] {

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

        def failure(name: String, topic: Topic) = {
          resultCounter.labels(topic, name, "failure").inc()
        }
      }
    }
  }
}