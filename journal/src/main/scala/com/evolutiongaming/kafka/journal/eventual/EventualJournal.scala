package com.evolutiongaming.kafka.journal.eventual

import com.evolutiongaming.kafka.journal.Alias._
import com.evolutiongaming.kafka.journal.FutureHelper._
import com.evolutiongaming.kafka.journal.LogHelper._
import com.evolutiongaming.kafka.journal.SeqRange
import com.evolutiongaming.safeakka.actor.ActorLog
import com.evolutiongaming.skafka.Topic

import scala.collection.immutable.Seq
import scala.concurrent.Future

trait EventualJournal {
  def topicPointers(topic: Topic): Future[TopicPointers]
  def read(id: Id, range: SeqRange): Future[Seq[EventualRecord]]
  def lastSeqNr(id: Id, from: SeqNr): Future[SeqNr]
}

object EventualJournal {

  val Empty: EventualJournal = {
    val futureTopicPointers = TopicPointers.Empty.future
    new EventualJournal {
      def topicPointers(topic: Topic) = futureTopicPointers
      def read(id: Id, range: SeqRange) = Future.seq
      def lastSeqNr(id: Id, from: SeqNr) = Future.seqNr
    }
  }


  def apply(eventualJournal: EventualJournal, log: ActorLog): EventualJournal = new EventualJournal {

    def topicPointers(topic: Topic): Future[TopicPointers] = {
      log[TopicPointers](s"topicPointers $topic") {
        eventualJournal.topicPointers(topic)
      }
    }

    def read(id: Id, range: SeqRange): Future[Seq[EventualRecord]] = {
      val toStr = (entries: Seq[EventualRecord]) => {
        entries.map(_.seqNr).mkString(",") // TODO use range and implement misses verification
      }
      log[Seq[EventualRecord]](s"$id read range: $range", toStr) {
        eventualJournal.read(id, range)
      }
    }

    def lastSeqNr(id: Id, from: SeqNr) = {
      log[SeqNr](s"$id lastSeqNr from: $from") {
        eventualJournal.lastSeqNr(id, from)
      }
    }
  }
}
