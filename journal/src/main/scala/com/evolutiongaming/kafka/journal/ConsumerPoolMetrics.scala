package com.evolutiongaming.kafka.journal

import cats.Applicative
import cats.effect.Resource
import cats.syntax.all._
import com.evolutiongaming.smetrics._
import com.evolutiongaming.smetrics.MetricsHelper._

import scala.concurrent.duration.FiniteDuration

trait ConsumerPoolMetrics[F[_]] {

  def acquire(latency: FiniteDuration): F[Unit]

  def use(latency: FiniteDuration): F[Unit]
}

object ConsumerPoolMetrics {

  def of[F[_]](
    registry: CollectorRegistry[F],
    prefix: String = "journal"
  ): Resource[F, ConsumerPoolMetrics[F]] = {

    registry
      .summary(
        name = s"${ prefix }_consumer_pool_latency",
        help = "Latency of acquiring/using consumers",
        quantiles = Quantiles.Default,
        labels = LabelNames("type"))
      .map { timeSummary =>
        class Main
        new Main with ConsumerPoolMetrics[F] {

          def observe(name: String, latency: FiniteDuration) =
            timeSummary
              .labels(name)
              .observe(latency.toNanos.nanosToSeconds)

          def acquire(latency: FiniteDuration) =
            observe("acquire", latency)

          def use(latency: FiniteDuration) =
            observe("use", latency)
        }
      }
  }

  def empty[F[_] : Applicative]: ConsumerPoolMetrics[F] = const(().pure[F])

  def const[F[_]](unit: F[Unit]): ConsumerPoolMetrics[F] = {
    class Const
    new Const with ConsumerPoolMetrics[F] {

      def acquire(latency: FiniteDuration) = unit

      def use(latency: FiniteDuration) = unit
    }
  }
}
