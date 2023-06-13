package com.evolutiongaming.kafka.journal

import cats.effect._
import com.evolutiongaming.catshelper.{MeasureDuration, ToFuture, ToTry}
import com.evolutiongaming.skafka
import com.evolutiongaming.skafka.consumer.{ConsumerConfig, ConsumerMetrics, ConsumerOf}
import com.evolutiongaming.smetrics

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

  @deprecated("Use `apply1` instead", "0.2.1")
  def apply[F[_]: Concurrent: ContextShift: Timer: ToTry: ToFuture: smetrics.MeasureDuration](
    executorBlocking: ExecutionContext,
    metrics: Option[ConsumerMetrics[F]] = None
  ): KafkaConsumerOf[F] = {
    implicit val md: MeasureDuration[F] = smetrics.MeasureDuration[F].toCatsHelper
    apply1(executorBlocking, metrics)
  }

  def apply1[F[_]: Concurrent: ContextShift: Timer: ToTry: ToFuture: MeasureDuration](
    executorBlocking: ExecutionContext,
    metrics: Option[ConsumerMetrics[F]] = None
  ): KafkaConsumerOf[F] = {

    val consumerOf = ConsumerOf.apply2(executorBlocking, metrics)
    apply(consumerOf)
  }


  def apply[F[_]: Concurrent](consumerOf: ConsumerOf[F]): KafkaConsumerOf[F] = {
    class Main
    new Main with KafkaConsumerOf[F] {

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
}
