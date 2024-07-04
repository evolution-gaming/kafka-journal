package com.evolutiongaming.kafka.journal

import cats.FlatMap
import cats.syntax.all.*
import com.evolutiongaming.skafka
import com.evolutiongaming.skafka.producer
import com.evolutiongaming.skafka.producer.*

trait KafkaProducer[F[_]] {

  def send[K, V](
    record: ProducerRecord[K, V],
  )(implicit toBytesK: skafka.ToBytes[F, K], toBytesV: skafka.ToBytes[F, V]): F[producer.RecordMetadata]
}

object KafkaProducer {

  def apply[F[_]](implicit F: KafkaProducer[F]): KafkaProducer[F] = F

  def apply[F[_]: FlatMap](producer: Producer[F]): KafkaProducer[F] = {
    new KafkaProducer[F] {

      def send[K, V](record: ProducerRecord[K, V])(implicit toBytesK: skafka.ToBytes[F, K], toBytesV: skafka.ToBytes[F, V]) = {
        producer.send(record).flatten
      }
    }
  }
}
