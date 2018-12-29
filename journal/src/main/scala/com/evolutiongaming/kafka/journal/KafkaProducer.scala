package com.evolutiongaming.kafka.journal

class KafkaProducer[F[_]] {

}

object KafkaProducer {
  def apply[F[_]](implicit F: KafkaProducer[F]): KafkaProducer[F] = F
}
