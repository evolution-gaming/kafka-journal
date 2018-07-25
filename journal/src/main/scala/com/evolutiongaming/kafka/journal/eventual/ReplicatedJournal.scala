package com.evolutiongaming.kafka.journal.eventual

import com.evolutiongaming.concurrent.async.Async
import com.evolutiongaming.kafka.journal.Key
import com.evolutiongaming.skafka.Topic


trait ReplicatedJournal {
  def topics(): Async[List[Topic]]
  def save(key: Key, records: UpdateTmp): Async[Unit]
  def savePointers(topic: Topic, topicPointers: TopicPointers): Async[Unit]
  def pointers(topic: Topic): Async[TopicPointers]
}