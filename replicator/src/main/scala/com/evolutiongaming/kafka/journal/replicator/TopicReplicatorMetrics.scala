package com.evolutiongaming.kafka.journal.replicator

import cats.effect._
import cats.implicits._
import cats.{Applicative, Monad}
import com.evolutiongaming.skafka.{Bytes => _, _}
import com.evolutiongaming.smetrics.MetricsHelper._
import com.evolutiongaming.smetrics._

import scala.concurrent.duration._


trait TopicReplicatorMetrics[F[_]] {
  import TopicReplicatorMetrics._

  def append(events: Int, bytes: Long, measurements: Measurements): F[Unit]

  def delete(measurements: Measurements): F[Unit]

  def purge(measurements: Measurements): F[Unit]

  def round(latency: FiniteDuration, records: Int): F[Unit]
}

object TopicReplicatorMetrics {

  def apply[F[_]](implicit F: TopicReplicatorMetrics[F]): TopicReplicatorMetrics[F] = F

  def empty[F[_] : Applicative]: TopicReplicatorMetrics[F] = const(Applicative[F].unit)

  def const[F[_]](unit: F[Unit]): TopicReplicatorMetrics[F] = new TopicReplicatorMetrics[F] {

    def append(events: Int, bytes: Long, measurements: Measurements) = unit

    def delete(measurements: Measurements) = unit

    def purge(measurements: Measurements) = unit

    def round(duration: FiniteDuration, records: Int) = unit
  }


  def of[F[_] : Monad](
    registry: CollectorRegistry[F],
    prefix: String = "replicator"
  ): Resource[F, Topic => TopicReplicatorMetrics[F]] = {

    val quantiles = Quantiles(
      Quantile(0.9, 0.05),
      Quantile(0.99, 0.005))

    val replicationSummary = registry.summary(
      name      = s"${ prefix }_replication_latency",
      help      = "Replication latency in seconds",
      quantiles = quantiles,
      labels    = LabelNames("topic", "type"))

    val deliverySummary = registry.summary(
      name      = s"${ prefix }_delivery_latency",
      help      = "Delivery latency in seconds",
      quantiles = quantiles,
      labels    = LabelNames("topic", "type"))

    val eventsSummary = registry.summary(
      name      = s"${ prefix }_events",
      help      = "Number of events replicated",
      quantiles = Quantiles.Empty,
      labels    = LabelNames("topic"))

    val bytesSummary = registry.summary(
      name      = s"${ prefix }_bytes",
      help      = "Number of bytes replicated",
      quantiles = Quantiles.Empty,
      labels    = LabelNames("topic"))

    val recordsSummary = registry.summary(
      name      = s"${ prefix }_records",
      help      = "Number of kafka records processed",
      quantiles = Quantiles.Empty,
      labels    = LabelNames("topic"))

    val roundSummary = registry.summary(
      name      = s"${ prefix }_round_duration",
      help      = "Replication round duration",
      quantiles = quantiles,
      labels    = LabelNames("topic"))

    val roundRecordsSummary = registry.summary(
      name      = s"${ prefix }_round_records",
      help      = "Number of kafka records processed in round",
      quantiles = Quantiles.Empty,
      labels    = LabelNames("topic"))

    for {
      replicationSummary  <- replicationSummary
      deliverySummary     <- deliverySummary
      eventsSummary       <- eventsSummary
      bytesSummary        <- bytesSummary
      recordsSummary      <- recordsSummary
      roundSummary        <- roundSummary
      roundRecordsSummary <- roundRecordsSummary
    } yield {

      topic: Topic => {

        def observeMeasurements(name: String, measurements: Measurements) = {
          for {
            _ <- replicationSummary.labels(topic, name).observe(measurements.replicationLatency.toNanos.nanosToSeconds)
            _ <- deliverySummary.labels(topic, name).observe(measurements.deliveryLatency.toNanos.nanosToSeconds)
            _ <- recordsSummary.labels(topic).observe(measurements.records.toDouble)
          } yield {}
        }

        new TopicReplicatorMetrics[F] {

          def append(events: Int, bytes: Long, measurements: Measurements) = {
            for {
              _ <- observeMeasurements("append", measurements)
              _ <- eventsSummary.labels(topic).observe(events.toDouble)
              _ <- bytesSummary.labels(topic).observe(bytes.toDouble)
            } yield {}
          }

          def delete(measurements: Measurements) = {
            observeMeasurements("delete", measurements)
          }

          def purge(measurements: Measurements) = {
            observeMeasurements("purge", measurements)
          }

          def round(duration: FiniteDuration, records: Int) = {
            for {
              _ <- roundSummary.labels(topic).observe(duration.toNanos.nanosToSeconds)
              _ <- roundRecordsSummary.labels(topic).observe(records.toDouble)
            } yield {}
          }
        }
      }
    }
  }


  final case class Measurements(
    replicationLatency: FiniteDuration,
    deliveryLatency: FiniteDuration,
    records: Int)
}