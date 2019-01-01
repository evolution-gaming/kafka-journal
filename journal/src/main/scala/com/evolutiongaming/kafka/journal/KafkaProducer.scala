package com.evolutiongaming.kafka.journal

import cats.effect.{ContextShift, Resource, Sync}
import cats.implicits._
import com.evolutiongaming.kafka.journal.util.FromFuture
import com.evolutiongaming.skafka.producer.{Producer, ProducerConfig, ProducerRecord, RecordMetadata}
import com.evolutiongaming.skafka

import scala.concurrent.ExecutionContext

trait KafkaProducer[F[_]] {

  def send[K : skafka.ToBytes, V : skafka.ToBytes](record: ProducerRecord[K, V]): F[RecordMetadata]
}

object KafkaProducer {

  def apply[F[_]](implicit F: KafkaProducer[F]): KafkaProducer[F] = F

  def of[F[_] : Sync : FromFuture : ContextShift](
    config: ProducerConfig,
    blocking: ExecutionContext,
    metrics: Option[Producer.Metrics] = None): Resource[F, KafkaProducer[F]] = {

    Resource {
      for {
        producer0 <- ContextShift[F].evalOn(blocking) {
          Sync[F].delay { Producer(config, blocking) }
        }
      } yield {

        val producer = metrics.fold(producer0) { metrics => Producer(producer0, metrics) }

        val release = for {
          _ <- FromFuture[F].apply { producer.flush() }
          _ <- FromFuture[F].apply { producer.close() }
        } yield {}

        val result = new KafkaProducer[F] {

          def send[K : skafka.ToBytes, V : skafka.ToBytes](record: ProducerRecord[K, V]) = {
            FromFuture[F].apply {
              producer.send(record)
            }
          }
        }
        (result, release)
      }
    }
  }
}
