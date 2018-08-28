package com.evolutiongaming.kafka.journal.eventual

import java.time.Instant

import com.evolutiongaming.kafka.journal.Key
import com.evolutiongaming.skafka.Topic


trait ReplicatedJournal[F[_]] {
  // TODO not used
  def topics(): F[Iterable[Topic]]

  def pointers(topic: Topic): F[TopicPointers]

  def save(key: Key, records: Replicate, timestamp: Instant): F[Unit]

  def save(topic: Topic, pointers: TopicPointers): F[Unit]
}

object ReplicatedJournal {
  def apply[F[_]](implicit F: ReplicatedJournal[F]): ReplicatedJournal[F] = F
}