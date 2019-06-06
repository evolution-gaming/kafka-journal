package com.evolutiongaming.kafka.journal

import cats.effect._
import com.evolutiongaming.catshelper.FromFuture
import com.evolutiongaming.skafka.producer.{ProducerConfig, ProducerMetrics}

import scala.concurrent.ExecutionContext


trait KafkaProducerOf[F[_]] {
  def apply(config: ProducerConfig): Resource[F, KafkaProducer[F]]
}

object KafkaProducerOf {

  def apply[F[_]](implicit F: KafkaProducerOf[F]): KafkaProducerOf[F] = F

  def apply[F[_] : Async : ContextShift : Clock : FromFuture](
    blocking: ExecutionContext,
    metrics: Option[ProducerMetrics[F]] = None
  ): KafkaProducerOf[F] = new KafkaProducerOf[F] {

    def apply(config: ProducerConfig) = {
      KafkaProducer.of[F](config, blocking, metrics)
    }
  }
}

