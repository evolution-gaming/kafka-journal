package com.evolutiongaming.kafka.journal

import cats.effect.{Concurrent, ContextShift, Resource}
import com.evolutiongaming.catshelper.FromFuture
import com.evolutiongaming.skafka
import com.evolutiongaming.skafka.consumer.{Consumer, ConsumerConfig}

import scala.concurrent.ExecutionContext

trait KafkaConsumerOf[F[_]] {
  def apply[K: skafka.FromBytes, V: skafka.FromBytes](config: ConsumerConfig): Resource[F, KafkaConsumer[F, K, V]]
}

object KafkaConsumerOf {

  def apply[F[_]](implicit F: KafkaConsumerOf[F]): KafkaConsumerOf[F] = F

  def apply[F[_] : Concurrent : ContextShift : FromFuture](
    blocking: ExecutionContext,
    metrics: Option[Consumer.Metrics] = None): KafkaConsumerOf[F] = {

    new KafkaConsumerOf[F] {

      def apply[K: skafka.FromBytes, V: skafka.FromBytes](config: ConsumerConfig) = {
        KafkaConsumer.of[F, K, V](config, blocking, metrics)
      }
    }
  }
}
