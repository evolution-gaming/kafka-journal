package com.evolutiongaming.kafka.journal

import java.time.Instant

import akka.actor.ActorSystem
import com.evolutiongaming.concurrent.async.Async
import com.evolutiongaming.concurrent.async.AsyncConverters._
import com.evolutiongaming.kafka.journal.Alias._
import com.evolutiongaming.kafka.journal.AsyncHelper._
import com.evolutiongaming.kafka.journal.FoldWhileHelper._
import com.evolutiongaming.kafka.journal.eventual.EventualJournal
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.safeakka.actor.ActorLog
import com.evolutiongaming.skafka.consumer.Consumer
import com.evolutiongaming.skafka.producer.Producer

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

// TODO consider passing topic along with id as method argument
// TODO consider replacing many methods with single `apply[In, Out]`
trait Journals {
  def append(key: Key, events: Nel[Event], timestamp: Instant): Async[Unit]
  def foldWhile[S](key: Key, from: SeqNr, s: S)(f: Fold[S, Event]): Async[S]
  def lastSeqNr(key: Key, from: SeqNr): Async[SeqNr]
  def delete(key: Key, to: SeqNr, timestamp: Instant): Async[Unit]
}

object Journals {

  val Empty: Journals = new Journals {
    def append(key: Key, events: Nel[Event], timestamp: Instant) = Async.unit
    def foldWhile[S](key: Key, from: SeqNr, s: S)(f: Fold[S, Event]) = s.async
    def lastSeqNr(key: Key, from: SeqNr) = Async.seqNr
    def delete(key: Key, to: SeqNr, timestamp: Instant) = Async.unit
  }


  def apply(settings: Settings): Journals = ???


  def apply(
    producer: Producer,
    newConsumer: () => Consumer[String, Bytes],
    eventual: EventualJournal = EventualJournal.Empty,
    pollTimeout: FiniteDuration = 100.millis)(implicit
    system: ActorSystem,
    ec: ExecutionContext): Journals = {

    def journalOf(key: Key) = {
      val log = ActorLog(system, classOf[Journal]) prefixed key.id
      val journal = Journal(key, log, producer, newConsumer, eventual, pollTimeout)
      Journal(journal, log)
    }

    new Journals {

      def append(key: Key, events: Nel[Event], timestamp: Instant) = {
        val journal = journalOf(key)
        journal.append(events, timestamp)
      }

      def foldWhile[S](key: Key, from: SeqNr, s: S)(f: Fold[S, Event]) = {
        val journal = journalOf(key)
        journal.foldWhile(from, s)(f)
      }

      def lastSeqNr(key: Key, from: SeqNr) = {
        val journal = journalOf(key)
        journal.lastSeqNr(from)
      }

      def delete(key: Key, to: SeqNr, timestamp: Instant) = {
        val journal = journalOf(key)
        journal.delete(to, timestamp)
      }
    }
  }
}