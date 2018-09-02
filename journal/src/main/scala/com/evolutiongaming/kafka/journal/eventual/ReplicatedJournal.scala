package com.evolutiongaming.kafka.journal.eventual

import java.time.Instant

import com.evolutiongaming.kafka.journal.{Key, ReplicatedEvent, SeqNr}
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.skafka.Topic


trait ReplicatedJournal[F[_]] {
  // TODO not used
  def topics(): F[Iterable[Topic]]

  def pointers(topic: Topic): F[TopicPointers]

  def append(key: Key, timestamp: Instant, events: Nel[ReplicatedEvent], deleteTo: Option[SeqNr]): F[Unit]

  def delete(key: Key, timestamp: Instant, deleteTo: SeqNr, bound: Boolean): F[Unit]

  def save(topic: Topic, pointers: TopicPointers): F[Unit]
}

object ReplicatedJournal {
  def apply[F[_]](implicit F: ReplicatedJournal[F]): ReplicatedJournal[F] = F
}