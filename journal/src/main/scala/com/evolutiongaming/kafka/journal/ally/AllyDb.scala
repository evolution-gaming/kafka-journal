package com.evolutiongaming.kafka.journal.ally

import com.evolutiongaming.kafka.journal.Alias._

import scala.concurrent.Future
import com.evolutiongaming.kafka.journal.SeqRange
import com.evolutiongaming.skafka.{Offset, Partition, Topic}

import scala.collection.immutable.Seq


// TODO separate on read and write
trait AllyDb extends AllyDbRead {
  // TODO make sure all have the same id, so the segments work as expected
  def save(records: Seq[AllyRecord], topic: Topic): Future[Unit]

  def savePointer(topic: Topic, partition: Partition, offset: Offset): Future[Unit]

  def insertPointer(topic: Topic, partition: Partition, offset: Offset): Future[Unit]
}

trait AllyDbRead {
  def pointer(id: Id, from: SeqNr): Future[Option[Pointer]]
  def list(id: Id, range: SeqRange): Future[Seq[AllyRecord]]
}

object AllyDbRead {

  val Empty: AllyDbRead = {
    val futureOption = Future.successful(None)
    val futureVector = Future.successful(Seq.empty)
    new AllyDbRead {
      def pointer(id: Id, from: SeqNr) = futureOption
      def list(id: Id, range: SeqRange) = futureVector
    }
  }
}