package com.evolutiongaming.kafka.journal

import cats.effect.{ContextShift, Sync}
import cats.implicits._
import com.evolutiongaming.kafka.journal.util.FromFuture
import com.evolutiongaming.skafka.producer.{Producer, ProducerConfig, ProducerRecord, RecordMetadata}

import scala.concurrent.ExecutionContext

trait KafkaProducer[F[_]] {

  def send(record: ProducerRecord[Id, Bytes]): F[RecordMetadata]

  def close: F[Unit]
}

object KafkaProducer {

  def apply[F[_]](implicit F: KafkaProducer[F]): KafkaProducer[F] = F

  def of[F[_] : Sync : FromFuture : ContextShift](
    config: ProducerConfig,
    blocking: ExecutionContext,
    metrics: Option[Producer.Metrics] = None): F[KafkaProducer[F]] = {

    for {
      producer0 <- ContextShift[F].evalOn(blocking) {
        Sync[F].delay { Producer(config, blocking) }
      }
      producer   = metrics.fold(producer0) { metrics => Producer(producer0, metrics) }
    } yield {

      new KafkaProducer[F] {

        def send(record: ProducerRecord[Id, Bytes]) = {
          FromFuture[F].apply {
            producer.send(record)
          }
        }

        def close = {
          for {
            _ <- FromFuture[F].apply { producer.flush() }
            _ <- FromFuture[F].apply { producer.close() }
          } yield {}
        }
      }
    }
  }
}
