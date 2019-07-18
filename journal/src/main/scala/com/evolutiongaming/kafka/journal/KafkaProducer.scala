package com.evolutiongaming.kafka.journal

import cats.FlatMap
import cats.effect._
import cats.implicits._
import com.evolutiongaming.catshelper.FromFuture
import com.evolutiongaming.skafka
import com.evolutiongaming.skafka.producer._

import scala.concurrent.ExecutionContext

trait KafkaProducer[F[_]] {

  def send[K: skafka.ToBytes, V: skafka.ToBytes](record: ProducerRecord[K, V]): F[RecordMetadata]
}

object KafkaProducer {

  def apply[F[_]](implicit F: KafkaProducer[F]): KafkaProducer[F] = F

  def of[F[_] : Async : ContextShift : Clock : FromFuture](
    config: ProducerConfig,
    blocking: ExecutionContext,
    metrics: Option[ProducerMetrics[F]] = None
  ): Resource[F, KafkaProducer[F]] = {

    val result = for {
      ab <- Producer.of(config, blocking).allocated
    } yield {
      val (producer0, close) = ab
      val producer = metrics.fold(producer0)(producer0.withMetrics)
      val kafkaProducer = apply(producer)

      val release = for {
        _ <- producer.flush
        _ <- close
      } yield {}

      (kafkaProducer, release)
    }

    Resource(result)
  }


  def apply[F[_] : FlatMap](producer: Producer[F]): KafkaProducer[F] = {
    new KafkaProducer[F] {

      def send[K: skafka.ToBytes, V: skafka.ToBytes](record: ProducerRecord[K, V]) = {
        producer.send(record).flatten
      }
    }
  }
}
