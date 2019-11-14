package com.evolutiongaming.kafka.journal.replicator

import com.evolutiongaming.kafka.journal.KafkaConsumer
import com.evolutiongaming.skafka.{OffsetAndMetadata, TopicPartition}

trait Commit[F[_]] {

  def apply(offsets: Map[TopicPartition, OffsetAndMetadata]): F[Unit]
}

object Commit {

  def apply[F[_]](kafkaConsumer: KafkaConsumer[F, _, _]): Commit[F] = {
    offsets: Map[TopicPartition, OffsetAndMetadata] => {
      kafkaConsumer.commit(offsets)
    }
  }
}