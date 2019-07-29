package com.evolutiongaming.kafka.journal

import cats.Monad
import cats.effect._
import com.evolutiongaming.catshelper.FromFuture
import com.evolutiongaming.skafka.producer.{ProducerConfig, ProducerMetrics, ProducerOf}
import com.evolutiongaming.smetrics.MeasureDuration

import scala.concurrent.ExecutionContext


trait KafkaProducerOf[F[_]] {

  def apply(config: ProducerConfig): Resource[F, KafkaProducer[F]]
}

object KafkaProducerOf {

  def apply[F[_]](implicit F: KafkaProducerOf[F]): KafkaProducerOf[F] = F


  def apply[F[_] : Sync : ContextShift : FromFuture : MeasureDuration](
    blocking: ExecutionContext,
    metrics: Option[ProducerMetrics[F]] = None
  ): KafkaProducerOf[F] = {

    val producerOf = ProducerOf(blocking, metrics)
    apply(producerOf)
  }


  def apply[F[_] : Monad](producerOf: ProducerOf[F]): KafkaProducerOf[F] = {
    new KafkaProducerOf[F] {
      def apply(config: ProducerConfig) = {
        for {
          producer <- producerOf(config)
        } yield {
          KafkaProducer(producer)
        }
      }
    }
  }
}

