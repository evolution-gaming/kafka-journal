package com.evolutiongaming.kafka.journal

import cats.effect.{Clock, Concurrent, ContextShift, Resource}
import com.evolutiongaming.skafka
import com.evolutiongaming.skafka.consumer.{ConsumerConfig, ConsumerMetrics}

import scala.concurrent.ExecutionContext

trait KafkaConsumerOf[F[_]] {
  def apply[K: skafka.FromBytes, V: skafka.FromBytes](config: ConsumerConfig): Resource[F, KafkaConsumer[F, K, V]]
}

object KafkaConsumerOf {

  def apply[F[_]](implicit F: KafkaConsumerOf[F]): KafkaConsumerOf[F] = F

  def apply[F[_] : Concurrent : ContextShift : Clock](
    blocking: ExecutionContext,
    metrics: Option[ConsumerMetrics[F]] = None
  ): KafkaConsumerOf[F] = {

    new KafkaConsumerOf[F] {

      def apply[K: skafka.FromBytes, V: skafka.FromBytes](config: ConsumerConfig) = {
        KafkaConsumer.of[F, K, V](config, blocking, metrics)
      }
    }
  }
}
