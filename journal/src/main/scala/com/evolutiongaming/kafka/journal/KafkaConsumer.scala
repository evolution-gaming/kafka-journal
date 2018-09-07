package com.evolutiongaming.kafka.journal

import com.evolutiongaming.kafka.journal.Implicits._
import com.evolutiongaming.skafka.consumer.{Consumer, ConsumerRecords, OffsetAndMetadata}
import com.evolutiongaming.skafka.{Topic, TopicPartition}

import scala.concurrent.duration.FiniteDuration

trait KafkaConsumer[F[_]] {

  def subscribe(topic: Topic): F[Unit]

  def poll(): F[ConsumerRecords[String, Bytes]]

  def commit(offsets: Map[TopicPartition, OffsetAndMetadata]): F[Unit]

  //  def seek(topic: Topic, partitionOffsets: List[PartitionOffset]/*TODO type ?*/): F[Unit]

  def close(): F[Unit]
}

object KafkaConsumer {

  def apply[F[_] : IO : AdaptFuture](
    consumer: Consumer[String, Bytes],
    pollTimeout: FiniteDuration): KafkaConsumer[F] = new KafkaConsumer[F] {

    def subscribe(topic: Topic) = {
      IO[F].point(consumer.subscribe(List(topic), None))
    }

    def commit(offsets: Map[TopicPartition, OffsetAndMetadata]) = {
      consumer.commit(offsets).adapt
    }

    /*def seek(topic: Topic, partitionOffsets: List[PartitionOffset]): F[Unit] = {

      val topicPartitions = for {
        partitionOffset <- partitionOffsets
      } yield {
        TopicPartition(topic = topic, partition = partitionOffset.partition)
      }
      // TODO Hide behind KafkaConsumer.seek ?
      consumer.assign(topicPartitions)
      for {
        partitionOffset <- partitionOffsets
      } {
        val topicPartition = TopicPartition(topic = topic, partition = partitionOffset.partition)
        // partitionOffset.offset // TODO use Option
        if(partitionOffset.offset == 0l) consumer.seekToBeginning(List(topicPartition))
        else consumer.seek(topicPartition, partitionOffset.offset + 1l/*TODO cover +1 with test*/)
      }
      IO[F].unit
    }*/

    def poll() = {
      consumer.poll(pollTimeout).adapt
    }

    def close() = {
      consumer.close().adapt
    }
  }
}
