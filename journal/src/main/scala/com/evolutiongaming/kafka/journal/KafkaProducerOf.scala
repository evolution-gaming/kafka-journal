package com.evolutiongaming.kafka.journal

import cats.Monad
import cats.effect._
import com.evolutiongaming.catshelper.{MeasureDuration, ToTry}
import com.evolutiongaming.skafka.producer.{ProducerConfig, ProducerMetrics, ProducerOf}
import com.evolutiongaming.smetrics


trait KafkaProducerOf[F[_]] {

  def apply(config: ProducerConfig): Resource[F, KafkaProducer[F]]
}

object KafkaProducerOf {

  def apply[F[_]](implicit F: KafkaProducerOf[F]): KafkaProducerOf[F] = F

  @deprecated("Use `apply1` instead", "2.2.0")
  def apply[F[_]: Async: smetrics.MeasureDuration: ToTry](
    metrics: Option[ProducerMetrics[F]] = None
  ): KafkaProducerOf[F] = {
    implicit val md: MeasureDuration[F] = smetrics.MeasureDuration[F].toCatsHelper
    apply1(metrics)
  }

  def apply1[F[_]: Async: MeasureDuration: ToTry](
    metrics: Option[ProducerMetrics[F]] = None
  ): KafkaProducerOf[F] = {
    val producerOf = ProducerOf.apply2(metrics)
    apply(producerOf)
  }


  def apply[F[_]: Monad](producerOf: ProducerOf[F]): KafkaProducerOf[F] = {
    config: ProducerConfig => {
      for {
        producer <- producerOf(config)
      } yield {
        KafkaProducer(producer)
      }
    }
  }
}

