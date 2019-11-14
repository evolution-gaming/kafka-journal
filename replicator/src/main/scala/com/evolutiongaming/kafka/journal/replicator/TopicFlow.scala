package com.evolutiongaming.kafka.journal.replicator

import cats.data.{NonEmptyList => Nel}
import com.evolutiongaming.kafka.journal.replicator.SubscriptionFlow.Records
import com.evolutiongaming.skafka.Partition


trait TopicFlow[F[_]] {

  def assign(partitions: Nel[Partition]): F[Unit]

  def apply(records: Records): F[Unit]

  def revoke(partitions: Nel[Partition]): F[Unit]
}