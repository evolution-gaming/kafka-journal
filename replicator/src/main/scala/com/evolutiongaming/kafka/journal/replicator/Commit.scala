package com.evolutiongaming.kafka.journal.replicator

import cats.data.{NonEmptyMap => Nem}
import com.evolutiongaming.kafka.journal.KafkaConsumer
import com.evolutiongaming.skafka.{OffsetAndMetadata, TopicPartition}

trait Commit[F[_]] {

  def apply(offsets: Nem[TopicPartition, OffsetAndMetadata]): F[Unit]
}

object Commit {

  def apply[F[_]](kafkaConsumer: KafkaConsumer[F, _, _]): Commit[F] = {
    offsets: Nem[TopicPartition, OffsetAndMetadata] => {
      kafkaConsumer.commit(offsets)
    }
  }
}