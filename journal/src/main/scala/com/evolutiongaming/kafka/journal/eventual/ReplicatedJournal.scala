package com.evolutiongaming.kafka.journal.eventual

import com.evolutiongaming.concurrent.async.Async
import com.evolutiongaming.kafka.journal.Alias._
import com.evolutiongaming.skafka.Topic


trait ReplicatedJournal {
  // TODO make sure all have the same id, so the segments work as expected
  def save(id: Id, records: UpdateTmp, topic: Topic): Async[Unit]
  def savePointers(topic: Topic, topicPointers: TopicPointers): Async[Unit]
  def pointers(topic: Topic): Async[TopicPointers]
}