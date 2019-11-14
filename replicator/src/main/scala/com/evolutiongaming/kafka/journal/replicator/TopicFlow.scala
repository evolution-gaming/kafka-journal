package com.evolutiongaming.kafka.journal.replicator

import cats.data.{NonEmptyList => Nel}
import com.evolutiongaming.skafka.Partition
import com.evolutiongaming.skafka.consumer.ConsumerRecords
import scodec.bits.ByteVector


trait TopicFlow[F[_]] {

  def assign(partitions: Nel[Partition]): F[Unit]

  def apply(records: ConsumerRecords[String, ByteVector]): F[Unit]

  def revoke(partitions: Nel[Partition]): F[Unit]
}