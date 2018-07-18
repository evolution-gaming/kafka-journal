package com.evolutiongaming.kafka.journal.eventual

import com.evolutiongaming.concurrent.async.Async
import com.evolutiongaming.concurrent.async.AsyncConverters._
import com.evolutiongaming.kafka.journal.ActorLogHelper._
import com.evolutiongaming.kafka.journal.Alias._
import com.evolutiongaming.kafka.journal.AsyncHelper._
import com.evolutiongaming.kafka.journal.FoldWhileHelper._
import com.evolutiongaming.kafka.journal.ReplicatedEvent
import com.evolutiongaming.safeakka.actor.ActorLog
import com.evolutiongaming.skafka.Topic

trait EventualJournal {
  // TODO should return NONE when it is empty, otherwise we will seek to wrong offset
  def topicPointers(topic: Topic): Async[TopicPointers]
  def foldWhile[S](id: Id, from: SeqNr, s: S)(f: Fold[S, ReplicatedEvent]): Async[Switch[S]]
  def lastSeqNr(id: Id, from: SeqNr): Async[SeqNr]
}

object EventualJournal {

  val Empty: EventualJournal = {
    val asyncTopicPointers = TopicPointers.Empty.async
    new EventualJournal {
      def topicPointers(topic: Topic) = asyncTopicPointers
      def foldWhile[S](id: Id, from: SeqNr, state: S)(f: Fold[S, ReplicatedEvent]) = state.continue.async
      def lastSeqNr(id: Id, from: SeqNr) = Async.seqNr
    }
  }


  def apply(eventualJournal: EventualJournal, log: ActorLog): EventualJournal = new EventualJournal {

    def topicPointers(topic: Topic) = {
      log[TopicPointers](s"topicPointers $topic") {
        eventualJournal.topicPointers(topic)
      }
    }

    def foldWhile[S](id: Id, from: SeqNr, s: S)(f: Fold[S, ReplicatedEvent]) = {
      log[Switch[S]](s"$id foldWhile from: $from, state: $s") {
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
