package com.evolutiongaming.kafka.journal

import cats.Monad
import cats.effect.*
import com.evolutiongaming.catshelper.{MeasureDuration, ToTry}
import com.evolutiongaming.skafka.producer.{ProducerConfig, ProducerMetrics, ProducerOf}

trait KafkaProducerOf[F[_]] {

  def apply(config: ProducerConfig): Resource[F, KafkaProducer[F]]
}

object KafkaProducerOf {

  def apply[F[_]](implicit F: KafkaProducerOf[F]): KafkaProducerOf[F] = F

  def apply[F[_]: Async: MeasureDuration: ToTry](
      metrics: Option[ProducerMetrics[F]] = None,
  ): KafkaProducerOf[F] = {
    val producerOf = ProducerOf.apply1(metrics)
    apply(producerOf)
  }

  def apply[F[_]: Monad](producerOf: ProducerOf[F]): KafkaProducerOf[F] = { (config: ProducerConfig) =>
    {
      for {
        producer <- producerOf(config)
      } yield {
        KafkaProducer(producer)
      }
    }
  }
}
