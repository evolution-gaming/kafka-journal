package com.evolutiongaming.kafka.journal

import java.time.Instant
import java.util.UUID

import com.evolutiongaming.concurrent.async.Async
import com.evolutiongaming.concurrent.async.AsyncConverters._
import com.evolutiongaming.kafka.journal.ActorLogHelper._
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

// TODO should we return offset ?
trait Journal {

  def append(events: Nel[Event], timestamp: Instant): Async[Unit]

  def read[S](from: SeqNr, s: S)(f: Fold[S, Event]): Async[S]

  def lastSeqNr(from: SeqNr): Async[Option[SeqNr]]

  def delete(to: SeqNr, timestamp: Instant): Async[Unit]
}

object Journal {

  val Empty: Journal = new Journal {
    def append(events: Nel[Event], timestamp: Instant) = Async.unit
    def read[S](from: SeqNr, s: S)(f: Fold[S, Event]) = s.async
    def lastSeqNr(from: SeqNr) = Async.none
    def delete(to: SeqNr, timestamp: Instant) = Async.unit

    override def toString = s"Journal.Empty"
  }


  def apply(journal: Journal, log: ActorLog): Journal = new Journal {

    def append(events: Nel[Event], timestamp: Instant) = {

      def eventsStr = {
        val head = events.head.seqNr
        val last = events.last.seqNr
        SeqRange(head, last)
      }

      log[Unit](s"append $eventsStr, timestamp: $timestamp") {
        journal.append(events, timestamp)
      }
    }

    def read[S](from: SeqNr, s: S)(f: Fold[S, Event]) = {
      log[S](s"read from: $from, state: $s") {
        journal.read(from, s)(f)
      }
    }

    def lastSeqNr(from: SeqNr) = {
      log[Option[SeqNr]](s"lastSeqNr $from") {
        journal.lastSeqNr(from)
      }
    }

    def delete(to: SeqNr, timestamp: Instant) = {
      log[Unit](s"delete $to, timestamp: $timestamp") {
        journal.delete(to, timestamp)
      }
    }

    override def toString = journal.toString
  }

  // TODO create separate class IdAndTopic
  def apply(
    key: Key,
    log: ActorLog, // TODO remove
    producer: Producer,
    newConsumer: Topic => Consumer[String, Bytes],
    eventual: EventualJournal,
    pollTimeout: FiniteDuration,
    closeTimeout: FiniteDuration)(implicit
    ec: ExecutionContext): Journal = {

    val withReadKafka = WithReadActions(newConsumer, pollTimeout, closeTimeout, log)

    val writeAction = WriteAction(key, producer)

    apply(key, log, eventual, withReadKafka, writeAction)
  }


  def apply(
    key: Key,
    log: ActorLog,
    eventual: EventualJournal,
    withReadActions: WithReadActions,
    writeAction: WriteAction): Journal = {

    def mark(): Async[Marker] = {
      val id = UUID.randomUUID().toString
      val action = Action.Mark(id, Instant.now())
      for {
        partitionOffset <- writeAction(action)
      } yield {
        Marker(id, partitionOffset)
      }
    }

    def readActions(from: SeqNr): Async[FoldActions] = {
      val marker = mark()
      val topicPointers = eventual.pointers(key.topic)
      for {
        marker <- marker
        topicPointers <- topicPointers
      } yield {
        val offsetReplicated = topicPointers.pointers.get(marker.partitionOffset.partition)
        FoldActions(key, from, marker, offsetReplicated, withReadActions)
      }
    }

    new Journal {

      def append(events: Nel[Event], timestamp: Instant) = {
        val payload = EventsSerializer.toBytes(events)
        val range = SeqRange(from = events.head.seqNr, to = events.last.seqNr)
        val action = Action.Append(range, timestamp, payload)
        val result = writeAction(action)
        result.unit
      }

      // TODO add optimisation for ranges
      def read[S](from: SeqNr, s: S)(f: Fold[S, Event]) = {

        def replicatedSeqNr(from: SeqNr) = {
          val ss = (s, from, Option.empty[Offset])
          eventual.read(key, from, ss) { case ((s, _, _), replicated) =>
            val event = replicated.event
            val switch = f(s, event)
            val from = event.seqNr.next
            switch.map { s =>
              val offset = replicated.partitionOffset.offset
              (s, from, Some(offset))
            }
          }
        }

        def replicated(from: SeqNr) = {
          for {
            s <- eventual.read(key, from, s) { (s, replicated) => f(s, replicated.event) }
          } yield s.s
        }

        def onNonEmpty(deleteTo: Option[SeqNr], foldActions: FoldActions) = {

          def events(from: SeqNr, offset: Option[Offset], s: S) = {
            foldActions(offset, s) { case (s, action) =>
              action match {
                case action: Action.Append =>
                  if (action.range.to < from) s.continue
                  else {
                    val events = EventsSerializer.fromBytes(action.events)
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
            s <- if (switch.stop) s.async else events(from, offset, s)
          } yield s
        }

        for {
          readActions <- readActions(from)
          // TODO use range after eventualRecords
          // TODO prevent from reading calling consume twice!
          info <- readActions(None, JournalInfo.empty) { (info, action) => info(action.header).continue }
          result <- info match {
            case JournalInfo.Empty                 => replicated(from)
            case JournalInfo.NonEmpty(_, deleteTo) => onNonEmpty(deleteTo, readActions)
            // TODO test this case
            // TODO test case when deleteTo == Long.Max
            case JournalInfo.DeleteTo(deleteTo) =>

              val x = if (deleteTo == SeqNr.Max) deleteTo else deleteTo.next
              replicated(from max x)
          }
        } yield result
      }

      def lastSeqNr(from: SeqNr) = {
        // TODO reimplement, we don't need to call `eventual.lastSeqNr` without using it's offset
        for {
          readActions <- readActions(from)
          seqNrEventual = eventual.lastSeqNr(key, from)
          seqNr <- readActions(None /*TODO*/ , Option.empty[SeqNr]) { (seqNr, action) =>
            val result = action match {
              case action: Action.Append => Some(action.header.range.to)
              case action: Action.Delete => seqNr
            }
            result.continue
          }
          seqNrEventual <- seqNrEventual
        } yield {
          seqNrEventual max seqNr
        }
      }

      def delete(to: SeqNr, timestamp: Instant) = {
        val action = Action.Delete(to, timestamp)
        writeAction(action).unit
      }

      override def toString = s"Journal($key)"
    }
  }
}