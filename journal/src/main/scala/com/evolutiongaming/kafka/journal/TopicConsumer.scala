package com.evolutiongaming.kafka.journal

import cats.effect.{Concurrent, ContextShift, Resource}
import com.evolutiongaming.kafka.journal.util.FromFuture
import com.evolutiongaming.skafka.Topic
import com.evolutiongaming.skafka.consumer.{Consumer, ConsumerConfig}

import scala.concurrent.ExecutionContext


// TODO remove this
trait TopicConsumer[F[_]] {
  def apply(topic: Topic): Resource[F, KafkaConsumer[F, Id, Bytes]]
}

object TopicConsumer {

  def apply[F[_] : Concurrent : FromFuture : ContextShift](
    config: ConsumerConfig,
    blocking: ExecutionContext,
    metrics: Option[Consumer.Metrics] = None): TopicConsumer[F] = {

    val config1 = config.copy(groupId = None)

    new TopicConsumer[F] {

      def apply(topic: Topic) = {
        KafkaConsumer.of(config1, blocking, metrics)
      }
    }
  }
}