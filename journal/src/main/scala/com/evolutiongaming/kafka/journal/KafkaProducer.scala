package com.evolutiongaming.kafka.journal

import cats.FlatMap
import cats.implicits._
import com.evolutiongaming.skafka
import com.evolutiongaming.skafka.producer._

trait KafkaProducer[F[_]] {

  // TODO doe we need ToBytes ?
  def send[K, V](
    record: ProducerRecord[K, V])(implicit
    toBytesK: skafka.ToBytes[F, K],
    toBytesV: skafka.ToBytes[F, V]
  ): F[RecordMetadata]
}

object KafkaProducer {

  def apply[F[_]](implicit F: KafkaProducer[F]): KafkaProducer[F] = F

  def apply[F[_] : FlatMap](producer: Producer[F]): KafkaProducer[F] = {
    new KafkaProducer[F] {

      def send[K, V](
        record: ProducerRecord[K, V])(implicit
        toBytesK: skafka.ToBytes[F, K],
        toBytesV: skafka.ToBytes[F, V]
      ) = {
        producer.send(record).flatten
      }
    }
  }
}
