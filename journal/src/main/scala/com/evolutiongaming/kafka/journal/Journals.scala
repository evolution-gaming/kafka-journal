package com.evolutiongaming.kafka.journal

import java.time.Instant

import akka.actor.ActorSystem
import com.evolutiongaming.kafka.journal.Alias._
import com.evolutiongaming.kafka.journal.eventual.EventualJournal
import com.evolutiongaming.kafka.journal.FutureHelper._
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.safeakka.actor.ActorLog
import com.evolutiongaming.skafka.consumer.Consumer
import com.evolutiongaming.skafka.producer.Producer

import scala.collection.immutable.Seq
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

// TODO consider passing topic along with id as method argument
// TODO consider replacing many methods with single `apply[In, Out]`
trait Journals {
  def append(id: Id, events: Nel[Event], timestamp: Instant): Future[Unit]
  // TODO decide on return type
  def read(id: Id, range: SeqRange): Future[Seq[Event]]
  def lastSeqNr(id: Id, from: SeqNr): Future[SeqNr]
  def delete(id: Id, to: SeqNr, timestamp: Instant): Future[Unit]
}

object Journals {

  val Empty: Journals = new Journals {
    def append(id: Id, events: Nel[Event], timestamp: Instant) = Future.unit
    def read(id: Id, range: SeqRange): Future[List[Event]] = Future.nil
    def lastSeqNr(id: Id, from: SeqNr) = Future.seqNr
    def delete(id: Id, to: SeqNr, timestamp: Instant) = Future.unit
  }


  def apply(settings: Settings): Journals = ???


  def apply(
    producer: Producer,
    newConsumer: () => Consumer[String, Bytes],
    eventual: EventualJournal = EventualJournal.Empty,
    pollTimeout: FiniteDuration = 100.millis)(implicit
    system: ActorSystem,
    ec: ExecutionContext): Journals = {

    def journalOf(id: Id) = {
      val topic = "journal"
      val log = ActorLog(system, classOf[Journal]) prefixed id
      val journal = Journal(id, topic, log, producer, newConsumer, eventual, pollTimeout)
      Journal(journal, log)
    }

    new Journals {

      def append(id: Id, events: Nel[Event], timestamp: Instant) = {
        val journal = journalOf(id)
        journal.append(events, timestamp)
      }

      def read(id: Id, range: SeqRange) = {
        val journal = journalOf(id)
        journal.read(range)
      }

      def lastSeqNr(id: Id, from: SeqNr) = {
        val journal = journalOf(id)
        journal.lastSeqNr(from)
      }

      def delete(id: Id, to: SeqNr, timestamp: Instant) = {
        val journal = journalOf(id)
        journal.delete(to, timestamp)
      }
    }
  }
}