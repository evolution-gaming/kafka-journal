package com.evolution.kafka.journal

import cats.Applicative
import cats.effect.Resource
import cats.syntax.all.*
import com.evolutiongaming.smetrics.*
import com.evolutiongaming.smetrics.MetricsHelper.*

import scala.concurrent.duration.FiniteDuration

trait ConsumerPoolMetrics[F[_]] {

  def acquire(latency: FiniteDuration): F[Unit]

  def use(latency: FiniteDuration): F[Unit]
}

object ConsumerPoolMetrics {

  def make[F[_]](
    registry: CollectorRegistry[F],
    prefix: String = "journal",
  ): Resource[F, ConsumerPoolMetrics[F]] = {

    registry
      .summary(
        name = s"${ prefix }_consumer_pool_latency",
        help = "Latency of acquiring/using consumers",
        quantiles = Quantiles.Default,
        labels = LabelNames("type"),
      )
      .map { timeSummary =>
        class Main
        new Main with ConsumerPoolMetrics[F] {

          def observe(name: String, latency: FiniteDuration): F[Unit] =
            timeSummary
              .labels(name)
              .observe(latency.toNanos.nanosToSeconds)

          def acquire(latency: FiniteDuration): F[Unit] =
            observe("acquire", latency)

          def use(latency: FiniteDuration): F[Unit] =
            observe("use", latency)
        }
      }
  }

  def empty[F[_]: Applicative]: ConsumerPoolMetrics[F] = const(().pure[F])

  def const[F[_]](unit: F[Unit]): ConsumerPoolMetrics[F] = {
    class Const
    new Const with ConsumerPoolMetrics[F] {

      def acquire(latency: FiniteDuration): F[Unit] = unit

      def use(latency: FiniteDuration): F[Unit] = unit
    }
  }
}
