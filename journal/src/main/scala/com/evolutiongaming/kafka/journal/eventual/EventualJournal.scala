package com.evolutiongaming.kafka.journal.eventual

import com.evolutiongaming.kafka.journal.Alias._
import com.evolutiongaming.kafka.journal.FutureHelper._
import com.evolutiongaming.kafka.journal.SeqRange
import com.evolutiongaming.skafka.Topic

import scala.collection.immutable.Seq
import scala.concurrent.Future

// TODO find better name
trait EventualJournal {
  def topicPointers(topic: Topic): Future[TopicPointers]
  def list(id: Id, range: SeqRange): Future[Seq[EventualRecord]]
  def lastSeqNr(id: Id, from: SeqNr): Future[Option[SeqNr]]
}

object EventualJournal {
  val Empty: EventualJournal = {
    val futureTopicPointers = Future.successful(TopicPointers.Empty)
    new EventualJournal {
      def topicPointers(topic: Topic) = futureTopicPointers
      def list(id: Id, range: SeqRange) = Future.seq
      def lastSeqNr(id: Id, from: SeqNr) = Future.none
    }
  }
}
