package com.evolutiongaming.kafka.journal

import com.evolutiongaming.kafka.journal.Implicits._
import com.evolutiongaming.skafka.consumer.{Consumer, ConsumerRecords}
import com.evolutiongaming.skafka.{Topic, TopicPartition}

import scala.concurrent.duration.FiniteDuration

trait KafkaConsumer[F[_]] {

//  def subscribe(topic: Topic): F[Unit]

  def poll(): F[ConsumerRecords[String, Bytes]]

  def seek(topic: Topic, partitionOffsets: List[PartitionOffset]/*TODO type ?*/): F[Unit]

  def close(): F[Unit]
}

object KafkaConsumer {

  def apply[F[_] : IO : AdaptFuture](
    consumer: Consumer[String, Bytes],
    pollTimeout: FiniteDuration): KafkaConsumer[F] = new KafkaConsumer[F] {

//    def subscribe(topic: Topic) = {
//      IO[F].point(consumer.subscribe(List(topic), None))
//    }

    def seek(topic: Topic, partitionOffsets: List[PartitionOffset]): F[Unit] = {
      for {
        partitionOffset <- partitionOffsets
      } {
        val topicPartition = TopicPartition(topic = topic, partition = partitionOffset.partition)
        consumer.seek(topicPartition, partitionOffset.offset)
      }
      IO[F].unit
    }

    def poll() = {
      consumer.poll(pollTimeout).adapt
    }

    def close() = {
      consumer.close().adapt
    }
  }
}
