package com.evolutiongaming.kafka.journal

import java.time.Instant
import java.util.UUID

import akka.actor.ActorSystem
import com.evolutiongaming.concurrent.async.Async
import com.evolutiongaming.concurrent.async.AsyncConverters._
import com.evolutiongaming.kafka.journal.ActorLogHelper._
import com.evolutiongaming.kafka.journal.EventsSerializer._
import com.evolutiongaming.kafka.journal.FoldWhileHelper._
import com.evolutiongaming.kafka.journal.SeqNr.Helper._
import com.evolutiongaming.kafka.journal.eventual.EventualJournal
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.safeakka.actor.ActorLog
import com.evolutiongaming.skafka.consumer.Consumer
import com.evolutiongaming.skafka.producer.Producer
import com.evolutiongaming.skafka.{Bytes => _, _}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

trait Journal {

  def append(key: Key, events: Nel[Event], timestamp: Instant): Async[PartitionOffset] // TODO add Source to RESULT, also rename usages

  def read[S](key: Key, from: SeqNr, s: S)(f: Fold[S, Event]): Async[S]

  def lastSeqNr(key: Key, from: SeqNr): Async[Option[SeqNr]]

  def delete(key: Key, to: SeqNr, timestamp: Instant): Async[PartitionOffset]
}

object Journal {

  val Empty: Journal = new Journal {

    def append(key: Key, events: Nel[Event], timestamp: Instant) = Async(PartitionOffset.Empty)

    def read[S](key: Key, from: SeqNr, s: S)(f: Fold[S, Event]) = s.async

    def lastSeqNr(key: Key, from: SeqNr) = Async.none

    def delete(key: Key, to: SeqNr, timestamp: Instant) = Async(PartitionOffset.Empty)

    override def toString = s"Journal.Empty"
  }

  def apply(journal: Journal, log: ActorLog): Journal = new Journal {

    def append(key: Key, events: Nel[Event], timestamp: Instant) = {

      def eventsStr = {
        val head = events.head.seqNr
        val last = events.last.seqNr
        SeqRange(head, last)
      }

      log[PartitionOffset](s"$key append $eventsStr, timestamp: $timestamp") {
        journal.append(key, events, timestamp)
      }
    }

    def read[S](key: Key, from: SeqNr, s: S)(f: Fold[S, Event]) = {
      log[S](s"$key read from: $from, state: $s") {
        journal.read(key, from, s)(f)
      }
    }

    def lastSeqNr(key: Key, from: SeqNr) = {
      log[Option[SeqNr]](s"$key lastSeqNr from: $from") {
        journal.lastSeqNr(key, from)
      }
    }

    def delete(key: Key, to: SeqNr, timestamp: Instant) = {
      log[PartitionOffset](s"$key delete to: $to, timestamp: $timestamp") {
        journal.delete(key, to, timestamp)
      }
    }

    override def toString = journal.toString
  }

  def apply(
    producer: Producer,
    origin: Option[Origin],
    consumerOf: Topic => Consumer[Id, Bytes],
    eventual: EventualJournal,
    pollTimeout: FiniteDuration = 100.millis,
    closeTimeout: FiniteDuration = 10.seconds)(implicit
    system: ActorSystem,
    ec: ExecutionContext): Journal = {

    val log = ActorLog(system, classOf[Journal])
    apply(log, origin, producer, consumerOf, eventual, pollTimeout, closeTimeout)
  }

  def apply(
    log: ActorLog, // TODO remove
    origin: Option[Origin],
    producer: Producer,
    consumerOf: Topic => Consumer[Id, Bytes],
    eventual: EventualJournal,
    pollTimeout: FiniteDuration,
    closeTimeout: FiniteDuration)(implicit
    ec: ExecutionContext): Journal = {

    val withReadActions = WithReadActions(consumerOf, pollTimeout, closeTimeout, log)

    val writeAction = AppendAction(producer)

    apply(log, origin, eventual, withReadActions, writeAction)
  }


  // TODO too many arguments, add config?
  def apply(
    log: ActorLog,
    origin: Option[Origin],
    eventual: EventualJournal,
    withReadActions: WithReadActions[Async],
    appendAction: AppendAction[Async]): Journal = {

    def mark(key: Key): Async[Marker] = {
      val id = UUID.randomUUID().toString
      val action = Action.Mark(key, Instant.now(), origin, id)
      for {
        partitionOffset <- appendAction(action)
      } yield {
        Marker(id, partitionOffset)
      }
    }

    def readActions(key: Key, from: SeqNr): Async[FoldActions] = {
      val marker = mark(key)
      val topicPointers = eventual.pointers(key.topic)
      for {
        marker <- marker
        topicPointers <- topicPointers
      } yield {
        log.debug(s"read $key topicPointers: $topicPointers")
        val offsetReplicated = topicPointers.values.get(marker.partitionOffset.partition)
        FoldActions(key, from, marker, offsetReplicated, withReadActions)
      }
    }

    new Journal {

      def append(key: Key, events: Nel[Event], timestamp: Instant) = {
        val action = Action.Append(key, timestamp, origin, events)
        appendAction(action)
      }

      // TODO add optimisation for ranges
      def read[S](key: Key, from: SeqNr, s: S)(f: Fold[S, Event]) = {

        def replicatedSeqNr(from: SeqNr) = {
          val ss: (S, Option[SeqNr], Option[Offset]) = (s, Some(from), None)
          eventual.read(key, from, ss) { case ((s, _, _), replicated) =>
            val event = replicated.event
            val switch = f(s, event)
            switch.map { s =>
              val offset = replicated.partitionOffset.offset
              val from = event.seqNr.next
              (s, from, Some(offset))
            }
          }
        }

        def replicated(from: SeqNr) = {
          for {
            s <- eventual.read(key, from, s) { (s, replicated) => f(s, replicated.event) }
          } yield s.s
        }

        def onNonEmpty(deleteTo: Option[SeqNr], readActions: FoldActions) = {

          def events(from: SeqNr, offset: Option[Offset], s: S) = {
            readActions(offset, s) { case (s, action) =>
              action match {
                case action: Action.Append =>
                  if (action.range.to < from) s.continue
                  else {
                    val events = EventsFromPayload(action.payload, action.payloadType)
                    events.foldWhile(s) { case (s, event) =>
                      if (event.seqNr >= from) f(s, event) else s.continue
                    }
                  }

                case action: Action.Delete => s.continue
              }
            }
          }


          val fromFixed = deleteTo.fold(from) { deleteTo => from max deleteTo.next }

          for {
            switch <- replicatedSeqNr(fromFixed)
            (s, from, offset) = switch.s
            _ = log.debug(s"read $key from: $from, offset: $offset")
            s <- from match {
              case None       => s.async
              case Some(from) => if (switch.stop) s.async else events(from, offset, s)
            }
          } yield s
        }

        for {
          readActions <- readActions(key, from)
          // TODO use range after eventualRecords
          // TODO prevent from reading calling consume twice!
          info <- readActions(None, JournalInfo.empty) { (info, action) => info(action).continue }
          _ = log.debug(s"read $key info: $info")
          result <- info match {
            case JournalInfo.Empty                 => replicated(from)
            case JournalInfo.NonEmpty(_, deleteTo) => onNonEmpty(deleteTo, readActions)
            // TODO test this case
            case JournalInfo.Deleted(deleteTo) => deleteTo.next match {
              case None       => s.async
              case Some(next) => replicated(from max next)
            }
          }
        } yield result
      }

      def lastSeqNr(key: Key, from: SeqNr) = {
        // TODO reimplement, we don't need to call `eventual.lastSeqNr` without using it's offset
        for {
          readActions <- readActions(key, from)
          seqNrEventual = eventual.lastSeqNr(key, from)
          seqNr <- readActions(None /*TODO*/ , Option.empty[SeqNr]) { (seqNr, action) =>
            val result = action match {
              case action: Action.Append => Some(action.range.to)
              case action: Action.Delete => seqNr
            }
            result.continue
          }
          seqNrEventual <- seqNrEventual
        } yield {
          seqNrEventual max seqNr
        }
      }

      def delete(key: Key, to: SeqNr, timestamp: Instant) = {
        val action = Action.Delete(key, timestamp, origin, to)
        appendAction(action)
      }
    }
  }
}