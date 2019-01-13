package com.evolutiongaming.kafka.journal

import cats.effect.Resource
import com.evolutiongaming.skafka.Topic
import com.evolutiongaming.skafka.consumer.ConsumerConfig


// TODO remove this
trait TopicConsumer[F[_]] {
  def apply(topic: Topic): Resource[F, KafkaConsumer[F, Id, Bytes]]
}

object TopicConsumer {

  def apply[F[_] : KafkaConsumerOf](config: ConsumerConfig): TopicConsumer[F] = {

    val config1 = config.copy(
      groupId = None,
      autoCommit = false)

    new TopicConsumer[F] {
      def apply(topic: Topic) = {
        KafkaConsumerOf[F].apply(config1)
      }
    }
  }
}