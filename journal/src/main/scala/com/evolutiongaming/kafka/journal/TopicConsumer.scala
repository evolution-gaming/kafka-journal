package com.evolutiongaming.kafka.journal

import com.evolutiongaming.skafka.Topic
import com.evolutiongaming.skafka.consumer.{Consumer, ConsumerConfig}

import scala.concurrent.{ExecutionContext, Future}

trait TopicConsumer {
  def apply(topic: Topic): Consumer[Id, Bytes, Future]
}

object TopicConsumer {

  def apply(
    config: ConsumerConfig,
    ecBlocking: ExecutionContext,
    metrics: Option[Consumer.Metrics] = None): TopicConsumer = new TopicConsumer {

    def apply(topic: Topic): Consumer[Id, Bytes, Future] = {
      val configFixed = config.copy(groupId = None)
      val consumer = Consumer[Id, Bytes](configFixed, ecBlocking)
      metrics.fold(consumer) { Consumer(consumer, _) }
    }
  }
}