package com.evolutiongaming.kafka.journal.eventual

import java.time.Instant

import com.evolutiongaming.concurrent.async.Async
import com.evolutiongaming.kafka.journal.Key
import com.evolutiongaming.skafka.Topic


trait ReplicatedJournal {
  // TODO not used
  def topics(): Async[Iterable[Topic]]

  def pointers(topic: Topic): Async[TopicPointers]

  def save(key: Key, records: Replicate, timestamp: Instant): Async[Unit]

  def save(topic: Topic, pointers: TopicPointers): Async[Unit]
}