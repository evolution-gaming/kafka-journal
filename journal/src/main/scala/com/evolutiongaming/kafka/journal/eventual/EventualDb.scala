package com.evolutiongaming.kafka.journal.eventual

import com.evolutiongaming.kafka.journal.Alias._

import scala.concurrent.Future
import com.evolutiongaming.kafka.journal.SeqRange
import com.evolutiongaming.kafka.journal.FutureHelper._
import com.evolutiongaming.skafka.{Offset, Partition, Topic}

import scala.collection.immutable.Seq


// TODO separate on read and write
trait EventualDb extends EventualDbRead {
  // TODO make sure all have the same id, so the segments work as expected
  def save(records: Seq[EventualRecord], topic: Topic): Future[Unit]

  def savePointers(updatePointers: UpdatePointers): Future[Unit]
}

trait EventualDbRead {
  def pointerOld(id: Id, from: SeqNr): Future[Option[Pointer]]
  def topicPointers(topic: Topic): Future[TopicPointers]
  def list(id: Id, range: SeqRange): Future[Seq[EventualRecord]]
  def lastSeqNr(id: Id, range: SeqRange): Future[Option[SeqNr]]
}

object EventualDbRead {

  val Empty: EventualDbRead = {
    val futureTopicPointers = Future.successful(TopicPointers.Empty)
    new EventualDbRead {
      def pointerOld(id: Id, from: SeqNr) = Future.none
      def topicPointers(topic: Topic) = futureTopicPointers
      def list(id: Id, range: SeqRange) = Future.seq
      def lastSeqNr(id: Id, range: SeqRange) = Future.none
    }
  }
}