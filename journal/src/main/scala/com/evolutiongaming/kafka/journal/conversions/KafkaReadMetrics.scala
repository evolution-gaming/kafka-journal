package com.evolutiongaming.kafka.journal.conversions

import cats.effect.Resource
import cats.{Applicative, Monad}
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.smetrics.MetricsHelper._
import com.evolutiongaming.smetrics.{CollectorRegistry, LabelNames, Quantile, Quantiles}

import scala.concurrent.duration.FiniteDuration

trait KafkaReadMetrics[F[_]] {

  def apply(payloadAndType: PayloadAndType, latency: FiniteDuration): F[Unit]
}

object KafkaReadMetrics {

  def empty[F[_]: Applicative]: KafkaReadMetrics[F] = (_, _) => Applicative[F].unit

  def of[F[_]: Monad](
    registry: CollectorRegistry[F],
    prefix: String = "journal"
  ): Resource[F, KafkaReadMetrics[F]] = {

    val durationSummary = registry.summary(
      name = s"${prefix}_payload_to_events_duration",
      help = "Journal payload to events conversion duration in seconds",
      quantiles = Quantiles(Quantile(0.9, 0.05), Quantile(0.99, 0.005)),
      labels = LabelNames("payload_type")
    )

    for {
      durationSummary <- durationSummary
    } yield {
      new KafkaReadMetrics[F] {

        def apply(payloadAndType: PayloadAndType, latency: FiniteDuration): F[Unit] =
          durationSummary
            .labels(payloadAndType.payloadType.name)
            .observe(latency.toNanos.nanosToSeconds)
      }
    }
  }
}
