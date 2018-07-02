package com.evolutiongaming.kafka.journal.ally

import com.evolutiongaming.kafka.journal.Alias._

import scala.concurrent.Future
import com.evolutiongaming.kafka.journal.SeqRange
import com.evolutiongaming.skafka.Topic

import scala.collection.immutable.Seq


// TODO separate on read and write
trait AllyDb extends AllyDbRead {
  // TODO make sure all have the same id, so the segments work as expected
  def save(records: Seq[AllyRecord], topic: Topic): Future[Unit]
}

trait AllyDbRead {
  // TODO we don't need payload here
  def last(id: Id, from: SeqNr): Future[Option[AllyRecord2]]

  def list(id: Id, range: SeqRange): Future[Seq[AllyRecord]]
}

object AllyDbRead {

  val Empty: AllyDbRead = {
    val futureOption = Future.successful(None)
    val futureVector = Future.successful(Seq.empty)
    new AllyDbRead {
      def last(id: Id, from: SeqNr) = futureOption
      def list(id: Id, range: SeqRange) = futureVector
    }
  }
}