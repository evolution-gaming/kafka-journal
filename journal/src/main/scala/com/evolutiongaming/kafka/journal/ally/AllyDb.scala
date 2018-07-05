package com.evolutiongaming.kafka.journal.ally

import com.evolutiongaming.kafka.journal.Alias._

import scala.concurrent.Future
import com.evolutiongaming.kafka.journal.SeqRange
import com.evolutiongaming.kafka.journal.FutureHelper._
import com.evolutiongaming.skafka.{Offset, Partition, Topic}

import scala.collection.immutable.Seq


// TODO separate on read and write
trait AllyDb extends AllyDbRead {
  // TODO make sure all have the same id, so the segments work as expected
  def save(records: Seq[AllyRecord], topic: Topic): Future[Unit]

  def savePointers(updatePointers: UpdatePointers): Future[Unit]
}

trait AllyDbRead {
  def pointerOld(id: Id, from: SeqNr): Future[Option[Pointer]]
  def topicPointers(topic: Topic): Future[TopicPointers]
  def list(id: Id, range: SeqRange): Future[Seq[AllyRecord]]
  def lastSeqNr(id: Id, range: SeqRange): Future[Option[SeqNr]]
}

object AllyDbRead {

  val Empty: AllyDbRead = {
    val futureTopicPointers = Future.successful(TopicPointers.Empty)
    new AllyDbRead {
      def pointerOld(id: Id, from: SeqNr) = Future.none
      def topicPointers(topic: Topic) = futureTopicPointers
      def list(id: Id, range: SeqRange) = Future.seq
      def lastSeqNr(id: Id, range: SeqRange) = Future.none
    }
  }
}