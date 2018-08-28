package com.evolutiongaming.kafka.journal

import com.evolutiongaming.skafka.producer.Producer

trait KafkaProducer[F[_]] {

}

object KafkaProducer {

  def apply[F[_]](producer: Producer, f: AdaptFuture[F]): KafkaProducer[F] = new KafkaProducer[F] {

  }
}
