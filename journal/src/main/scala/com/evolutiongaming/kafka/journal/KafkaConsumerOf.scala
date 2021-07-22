package com.evolutiongaming.kafka.journal

import cats.effect._
import com.evolutiongaming.catshelper.{ToFuture, ToTry}
import com.evolutiongaming.skafka
import com.evolutiongaming.skafka.consumer.{ConsumerConfig, ConsumerMetrics, ConsumerOf}
import com.evolutiongaming.smetrics.MeasureDuration

import scala.concurrent.ExecutionContext

trait KafkaConsumerOf[F[_]] {

  def apply[K, V](
    config: ConsumerConfig)(implicit
    fromBytesK: skafka.FromBytes[F, K],
    fromBytesV: skafka.FromBytes[F, V]
  ): Resource[F, KafkaConsumer[F, K, V]]
}

object KafkaConsumerOf {

  def apply[F[_]](implicit F: KafkaConsumerOf[F]): KafkaConsumerOf[F] = F

  def apply[F[_]: Concurrent: ContextShift: ToTry: ToFuture: MeasureDuration](
    executorBlocking: ExecutionContext,
    metrics: Option[ConsumerMetrics[F]] = None
  ): KafkaConsumerOf[F] = {

    val consumerOf = ConsumerOf(executorBlocking, metrics)
    apply(consumerOf)
  }


  private sealed abstract class Main

  def apply[F[_]: Concurrent](consumerOf: ConsumerOf[F]): KafkaConsumerOf[F] = new Main with KafkaConsumerOf[F] {

    def apply[K, V](
      config: ConsumerConfig)(implicit
      fromBytesK: skafka.FromBytes[F, K],
      fromBytesV: skafka.FromBytes[F, V]
    ) = {
      val consumer = consumerOf[K, V](config)
      KafkaConsumer.of(consumer)
    }
  }
}
