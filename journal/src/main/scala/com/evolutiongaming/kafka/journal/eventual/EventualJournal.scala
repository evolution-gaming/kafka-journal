package com.evolutiongaming.kafka.journal.eventual

import com.evolutiongaming.kafka.journal.Alias._
import com.evolutiongaming.kafka.journal.FoldWhileHelper._
import com.evolutiongaming.kafka.journal.FutureHelper._
import com.evolutiongaming.kafka.journal.LogHelper._
import com.evolutiongaming.safeakka.actor.ActorLog
import com.evolutiongaming.skafka.Topic

import scala.concurrent.Future

trait EventualJournal {
  def topicPointers(topic: Topic): Future[TopicPointers]
  def read[S](id: Id, from: SeqNr, s: S)(f: FoldWhile[S, EventualRecord]): Future[(S, Continue)]
  def lastSeqNr(id: Id, from: SeqNr): Future[SeqNr]
}

object EventualJournal {

  val Empty: EventualJournal = {
    val futureTopicPointers = TopicPointers.Empty.future
    new EventualJournal {
      def topicPointers(topic: Topic) = futureTopicPointers
      def read[S](id: Id, from: SeqNr, state: S)(f: FoldWhile[S, EventualRecord]) = (state, true).future
      def lastSeqNr(id: Id, from: SeqNr) = Future.seqNr
    }
  }


  def apply(eventualJournal: EventualJournal, log: ActorLog): EventualJournal = new EventualJournal {

    def topicPointers(topic: Topic): Future[TopicPointers] = {
      log[TopicPointers](s"topicPointers $topic") {
        eventualJournal.topicPointers(topic)
      }
    }

    def read[S](id: Id, from: SeqNr, s: S)(f: FoldWhile[S, EventualRecord]) = {

      val ff = (state: S, record: EventualRecord) => {
        val (result, continue) = f(state, record)
        log.debug(s"$id foldWhile record: $record, state: $state, result: $result, continue: $continue")
        (result, continue)
      }

      log[(S, Continue)](s"$id foldWhile from: $from, state: $s") {
        eventualJournal.read(id, from, s)(ff)
      }
    }

    def lastSeqNr(id: Id, from: SeqNr) = {
      log[SeqNr](s"$id lastSeqNr from: $from") {
        eventualJournal.lastSeqNr(id, from)
      }
    }
  }
}
