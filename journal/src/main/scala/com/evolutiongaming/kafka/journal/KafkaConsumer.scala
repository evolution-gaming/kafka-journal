package com.evolutiongaming.kafka.journal

import com.evolutiongaming.kafka.journal.IO.Implicits._
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.skafka.consumer.{Consumer, ConsumerRecords}
import com.evolutiongaming.skafka.{OffsetAndMetadata, Topic, TopicPartition}

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

trait KafkaConsumer[F[_]] {

  def subscribe(topic: Topic): F[Unit]

  def poll(): F[ConsumerRecords[Id, Bytes]]

  def commit(offsets: Map[TopicPartition, OffsetAndMetadata]): F[Unit]

  def close(): F[Unit]
}

object KafkaConsumer {

  def apply[F[_] : IO : AdaptFuture](
    consumer: Consumer[Id, Bytes, Future],
    pollTimeout: FiniteDuration): KafkaConsumer[F] = new KafkaConsumer[F] {

    def subscribe(topic: Topic) = {
      IO[F].point(consumer.subscribe(Nel(topic), None))
    }

    def commit(offsets: Map[TopicPartition, OffsetAndMetadata]) = {
      consumer.commit(offsets).adapt
    }

    def poll() = {
      consumer.poll(pollTimeout).adapt
    }

    def close() = {
      consumer.close().adapt
    }
  }
}
