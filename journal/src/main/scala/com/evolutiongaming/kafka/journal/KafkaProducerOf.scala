package com.evolutiongaming.kafka.journal

import cats.Monad
import cats.effect._
import cats.syntax.all._
import com.evolutiongaming.catshelper.{MeasureDuration, ToTry}
import com.evolutiongaming.skafka.producer.{ProducerConfig, ProducerMetrics, ProducerOf}
import com.evolutiongaming.skafka.metrics.KafkaMetricsCollector
import io.prometheus.client.CollectorRegistry


trait KafkaProducerOf[F[_]] {

  def apply(config: ProducerConfig): Resource[F, KafkaProducer[F]]
}

object KafkaProducerOf {

  def apply[F[_]](implicit F: KafkaProducerOf[F]): KafkaProducerOf[F] = F

  @deprecated("use apply1 with `prefix` param", "3.3.10")
  def apply[F[_]: Async: MeasureDuration: ToTry](
    metrics: Option[ProducerMetrics[F]] = None
  ): KafkaProducerOf[F] = {
    val producerOf = ProducerOf.apply1(metrics)
    apply(producerOf)
  }


  @deprecated("use apply1 with `prefix` param", "3.3.10")
  def apply[F[_]: Monad](producerOf: ProducerOf[F]): KafkaProducerOf[F] = {
    (config: ProducerConfig) => {
      for {
        producer <- producerOf(config)
      } yield {
        KafkaProducer(producer)
      }
    }
  }


  def apply1[F[_]: Async: MeasureDuration: ToTry](
    metrics: Option[ProducerMetrics[F]] = None,
    prefix: String
  ): KafkaProducerOf[F] = {
    val producerOf = ProducerOf.apply1(metrics)
    apply1(producerOf, prefix)
  }


  def apply1[F[_]: Monad: ToTry](producerOf: ProducerOf[F], metricPrefix: String): KafkaProducerOf[F] = {
    (config: ProducerConfig) => {
      for {
        producer <- producerOf(config)
      } yield {
        val collector = new KafkaMetricsCollector[F](producer.clientMetrics, metricPrefix.some)
        CollectorRegistry.defaultRegistry.register(collector)
        KafkaProducer(producer)
      }
    }
  }
}

