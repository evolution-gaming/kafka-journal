package com.evolutiongaming.kafka.journal

import cats.Applicative
import cats.effect.Resource
import cats.syntax.all._
import com.evolutiongaming.skafka._
import com.evolutiongaming.smetrics._
import com.evolutiongaming.smetrics.MetricsHelper._

import scala.concurrent.duration.FiniteDuration

trait ConsumerPoolMetrics[F[_]] {

  def acquireTime(topic: Topic, time: FiniteDuration): F[Unit]

  def useTime(topic: Topic, time: FiniteDuration): F[Unit]
}

object ConsumerPoolMetrics {

  def of[F[_]](
    registry: CollectorRegistry[F],
    prefix: String = "journal"
  ): Resource[F, ConsumerPoolMetrics[F]] = {

    val timeSummary = registry.summary(
      name = s"${prefix}_consumer_pool_time_spent",
      help = "Time spent acquiring/using consumers",
      quantiles = Quantiles.Default,
      labels = LabelNames("topic", "type")
    )

    for {
      timeSummary <- timeSummary
    } yield {
      class Main
      new Main with ConsumerPoolMetrics[F] {

        def observeLatency(name: String, topic: Topic, latency: FiniteDuration) =
          timeSummary.labels(topic, name).observe(latency.toNanos.nanosToSeconds)

        override def acquireTime(topic: Topic, time: FiniteDuration): F[Unit] =
          observeLatency("acquire", topic, time)

        override def useTime(topic: Topic, time: FiniteDuration): F[Unit] =
          observeLatency("use", topic, time)
      }
    }
  }

  def empty[F[_] : Applicative]: ConsumerPoolMetrics[F] = const(().pure[F])

  def const[F[_]](unit: F[Unit]): ConsumerPoolMetrics[F] = {
    class Const
    new Const with ConsumerPoolMetrics[F] {

      override def acquireTime(topic: Topic, time: FiniteDuration): F[Unit] = unit

      override def useTime(topic: Topic, time: FiniteDuration): F[Unit] = unit
    }
  }
}
