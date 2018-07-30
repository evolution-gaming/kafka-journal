package com.evolutiongaming.kafka.journal.eventual

import com.evolutiongaming.concurrent.async.Async
import com.evolutiongaming.concurrent.async.AsyncConverters._
import com.evolutiongaming.kafka.journal.ActorLogHelper._
import com.evolutiongaming.kafka.journal.FoldWhileHelper._
import com.evolutiongaming.kafka.journal.{Key, ReplicatedEvent, SeqNr}
import com.evolutiongaming.safeakka.actor.ActorLog
import com.evolutiongaming.skafka.Topic

trait EventualJournal {
  // TODO should return NONE when it is empty, otherwise we will seek to wrong offset
  def topicPointers(topic: Topic): Async[TopicPointers]
  // TODO don't we need to query by Key here ?
  def foldWhile[S](key: Key, from: SeqNr, s: S)(f: Fold[S, ReplicatedEvent]): Async[Switch[S]]
  def lastSeqNr(key: Key, from: SeqNr): Async[Option[SeqNr]]
}

object EventualJournal {

  val Empty: EventualJournal = {
    val asyncTopicPointers = TopicPointers.Empty.async
    new EventualJournal {
      def topicPointers(topic: Topic) = asyncTopicPointers
      def foldWhile[S](key: Key, from: SeqNr, state: S)(f: Fold[S, ReplicatedEvent]) = state.continue.async
      def lastSeqNr(key: Key, from: SeqNr) = Async.none
    }
  }


  def apply(journal: EventualJournal, log: ActorLog): EventualJournal = new EventualJournal {

    def topicPointers(topic: Topic) = {
      log[TopicPointers](s"topicPointers $topic") {
        journal.topicPointers(topic)
      }
    }

    def foldWhile[S](key: Key, from: SeqNr, s: S)(f: Fold[S, ReplicatedEvent]) = {
      log[Switch[S]](s"$key foldWhile from: $from, state: $s") {
        journal.foldWhile(key, from, s)(f)
      }
    }

    def lastSeqNr(key: Key, from: SeqNr) = {
      log[Option[SeqNr]](s"$key lastSeqNr from: $from") {
        journal.lastSeqNr(key, from)
      }
    }
  }
}
