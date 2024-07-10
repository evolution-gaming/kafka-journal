package com.evolutiongaming.kafka.journal

import cats.Monad
import cats.effect.*
import cats.syntax.all.*
import com.evolutiongaming.catshelper.{MeasureDuration, ToTry}
import com.evolutiongaming.skafka.producer.{ProducerConfig, ProducerMetrics, ProducerOf}
import com.evolutiongaming.skafka.metrics.KafkaMetricsCollector
import io.prometheus.client.CollectorRegistry

trait KafkaProducerOf[F[_]] {

  def apply(config: ProducerConfig): Resource[F, KafkaProducer[F]]
}

object KafkaProducerOf {

  def apply[F[_]](implicit F: KafkaProducerOf[F]): KafkaProducerOf[F] = F

  @deprecated("use apply1 with `prefix` param", "3.4.1")
  def apply[F[_]: Async: MeasureDuration: ToTry](
    metrics: Option[ProducerMetrics[F]] = None,
  ): KafkaProducerOf[F] = {
    val producerOf = ProducerOf.apply1(metrics)
    apply(producerOf)
  }

  @deprecated("use apply1 with `prefix` param", "3.4.1")
  def apply[F[_]: Monad](producerOf: ProducerOf[F]): KafkaProducerOf[F] = { (config: ProducerConfig) =>
    {
      for {
        producer <- producerOf(config)
      } yield {
        KafkaProducer(producer)
      }
    }
  }

  def apply1[F[_]: Async: MeasureDuration: ToTry](
    prefix: String,
    metrics: Option[ProducerMetrics[F]] = None,
  ): KafkaProducerOf[F] = {
    val producerOf = ProducerOf.apply1(metrics)
    apply1(producerOf, prefix)
  }

  def apply1[F[_]: Sync: ToTry](producerOf: ProducerOf[F], metricPrefix: String): KafkaProducerOf[F] = { config =>
    for {
      producer <- producerOf(config)
      collector = new KafkaMetricsCollector[F](producer.clientMetrics, metricPrefix.some)
      _ <- Resource.make {
        Sync[F].delay {
          CollectorRegistry.defaultRegistry.register(collector)
        }
      } { _ =>
        Sync[F].delay {
          CollectorRegistry.defaultRegistry.unregister(collector)
        }
      }
    } yield {
      KafkaProducer(producer)
    }
  }
}
