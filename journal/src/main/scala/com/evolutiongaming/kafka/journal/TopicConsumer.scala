package com.evolutiongaming.kafka.journal

import java.util.UUID

import com.evolutiongaming.skafka.Topic
import com.evolutiongaming.skafka.consumer.{Consumer, ConsumerConfig}

import scala.concurrent.ExecutionContext

trait TopicConsumer {
  def apply(topic: Topic): Consumer[Id, Bytes]
}

object TopicConsumer {

  def apply(
    config: ConsumerConfig,
    ecBlocking: ExecutionContext,
    metrics: Option[Consumer.Metrics] = None): TopicConsumer = {

    apply(
      config = config,
      ecBlocking = ecBlocking,
      prefix = config.groupId getOrElse "journal",
      metrics = metrics)
  }

  def apply(
    config: ConsumerConfig,
    ecBlocking: ExecutionContext,
    prefix: String,
    metrics: Option[Consumer.Metrics]): TopicConsumer = new TopicConsumer {

    def apply(topic: Topic): Consumer[Id, Bytes] = {
      val uuid = UUID.randomUUID()
      val groupId = s"$prefix-$topic-$uuid"
      val configFixed = config.copy(groupId = Some(groupId))
      val consumer = Consumer[Id, Bytes](configFixed, ecBlocking)
      metrics.fold(consumer) { Consumer(consumer, _) }
    }
  }
}