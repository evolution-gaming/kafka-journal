package com.evolutiongaming.kafka.journal.ally

import com.evolutiongaming.kafka.journal.Alias._

import scala.collection.immutable.Seq
import scala.concurrent.Future


// TODO separate on read and write
trait AllyDb extends AllyDbRead {
  def save(records: Seq[AllyRecord]): Future[Unit]
}

trait AllyDbRead {
  // TODO we don't need payload here
  def last(id: Id): Future[Option[AllyRecord2]]

  def list(id: Id): Future[Vector[AllyRecord]]
}

object AllyDbRead {

  val Empty: AllyDbRead = {
    val futureOption = Future.successful(None)
    val futureVector = Future.successful(Vector.empty)
    new AllyDbRead {
      def last(id: Id) = futureOption
      def list(id: Id) = futureVector
    }
  }
}