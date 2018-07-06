package com.evolutiongaming.kafka.journal.eventual

import com.evolutiongaming.kafka.journal.Alias._
import com.evolutiongaming.skafka.Topic

import scala.collection.immutable.Seq
import scala.concurrent.Future


// TODO rename
trait EventualDb {
  // TODO make sure all have the same id, so the segments work as expected
  def save(records: Seq[EventualRecord], topic: Topic): Future[Unit]
  def savePointers(updatePointers: UpdatePointers): Future[Unit]
  def pointerOld(id: Id, from: SeqNr): Future[Option[Pointer]]
  def topicPointers(topic: Topic): Future[TopicPointers]
}