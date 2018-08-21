package com.evolutiongaming.kafka.journal.eventual

import java.time.Instant

import com.evolutiongaming.concurrent.async.Async
import com.evolutiongaming.kafka.journal.Key
import com.evolutiongaming.skafka.Topic


trait ReplicatedJournal {
  // TODO not used
  def topics(): Async[Iterable[Topic]]

  def save(key: Key, records: Replicate, timestamp: Instant): Async[Unit]

  def savePointers(topic: Topic, topicPointers: TopicPointers): Async[Unit]

  def pointers(topic: Topic): Async[TopicPointers]
}