package com.evolutiongaming.kafka.journal.replicator

import cats.data.{NonEmptyList => Nel}
import com.evolutiongaming.kafka.journal.replicator.SubscriptionFlow.Records
import com.evolutiongaming.skafka.{Offset, Partition}


trait TopicFlow[F[_]] {

  def assign(partitions: Nel[Partition]): F[Unit]

  def apply(records: Records): F[Map[Partition, Offset]]

  def revoke(partitions: Nel[Partition]): F[Unit]
}