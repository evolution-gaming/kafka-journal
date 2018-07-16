package com.evolutiongaming.kafka.journal.eventual

import com.evolutiongaming.kafka.journal.Alias._
import com.evolutiongaming.kafka.journal.FoldWhileHelper._
import com.evolutiongaming.kafka.journal.FutureHelper._
import com.evolutiongaming.kafka.journal.LogHelper._
import com.evolutiongaming.kafka.journal.ReplicatedEvent
import com.evolutiongaming.safeakka.actor.ActorLog
import com.evolutiongaming.skafka.Topic

import scala.concurrent.Future

trait EventualJournal {
  // TODO should return NONE when it is empty, otherwise we will seek to wrong offset
  def topicPointers(topic: Topic): Future[TopicPointers]
  def foldWhile[S](id: Id, from: SeqNr, s: S)(f: Fold[S, ReplicatedEvent]): Future[(S, Continue)]
  def lastSeqNr(id: Id, from: SeqNr): Future[SeqNr]
}

object EventualJournal {

  val Empty: EventualJournal = {
    val futureTopicPointers = TopicPointers.Empty.future
    new EventualJournal {
      def topicPointers(topic: Topic) = futureTopicPointers
      def foldWhile[S](id: Id, from: SeqNr, state: S)(f: Fold[S, ReplicatedEvent]) = (state, true).future
      def lastSeqNr(id: Id, from: SeqNr) = Future.seqNr
    }
  }


  def apply(eventualJournal: EventualJournal, log: ActorLog): EventualJournal = new EventualJournal {

    def topicPointers(topic: Topic): Future[TopicPointers] = {
      log[TopicPointers](s"topicPointers $topic") {
        eventualJournal.topicPointers(topic)
      }
    }

    def foldWhile[S](id: Id, from: SeqNr, s: S)(f: Fold[S, ReplicatedEvent]) = {
      log[(S, Continue)](s"$id foldWhile from: $from, state: $s") {
        eventualJournal.foldWhile(id, from, s)(f)
      }
    }

    def lastSeqNr(id: Id, from: SeqNr) = {
      log[SeqNr](s"$id lastSeqNr from: $from") {
        eventualJournal.lastSeqNr(id, from)
      }
    }
  }
}
