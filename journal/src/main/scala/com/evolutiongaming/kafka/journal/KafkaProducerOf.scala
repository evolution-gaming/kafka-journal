package com.evolutiongaming.kafka.journal

import cats.effect.{ContextShift, Resource, Sync}
import com.evolutiongaming.kafka.journal.util.FromFuture
import com.evolutiongaming.skafka.producer.{Producer, ProducerConfig}

import scala.concurrent.ExecutionContext


trait KafkaProducerOf[F[_]] {
  def apply(config: ProducerConfig): Resource[F, KafkaProducer[F]]
}

object KafkaProducerOf {

  def apply[F[_]](implicit F: KafkaProducerOf[F]): KafkaProducerOf[F] = F

  def apply[F[_] : Sync : ContextShift : FromFuture](
    blocking: ExecutionContext,
    metrics: Option[Producer.Metrics] = None): KafkaProducerOf[F] = {

    new KafkaProducerOf[F] {
      def apply(config: ProducerConfig) = {
        KafkaProducer.of[F](config, blocking, metrics)
      }
    }
  }
}

