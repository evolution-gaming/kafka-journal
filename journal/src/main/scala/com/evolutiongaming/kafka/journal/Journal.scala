package com.evolutiongaming.kafka.journal

import java.time.Instant
import java.util.UUID

import com.evolutiongaming.concurrent.async.Async
import com.evolutiongaming.concurrent.async.AsyncConverters._
import com.evolutiongaming.kafka.journal.ActorLogHelper._
import com.evolutiongaming.kafka.journal.Alias._
import com.evolutiongaming.kafka.journal.AsyncHelper._
import com.evolutiongaming.kafka.journal.FoldWhileHelper._
import com.evolutiongaming.kafka.journal.eventual.EventualJournal
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.safeakka.actor.ActorLog
import com.evolutiongaming.skafka.consumer.Consumer
import com.evolutiongaming.skafka.producer.Producer
import com.evolutiongaming.skafka.{Bytes => _, _}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

// TODO consider passing topic along with id as method argument
// TODO should we return offset ?
trait Journal {
  def append(events: Nel[Event], timestamp: Instant): Async[Unit]
  def foldWhile[S](from: SeqNr, s: S)(f: Fold[S, Event]): Async[S]
  def lastSeqNr(from: SeqNr): Async[SeqNr]
  def delete(to: SeqNr, timestamp: Instant): Async[Unit]
}

object Journal {

  val Empty: Journal = new Journal {
    def append(events: Nel[Event], timestamp: Instant) = Async.unit
    def foldWhile[S](from: SeqNr, s: S)(f: Fold[S, Event]) = s.async
    def lastSeqNr(from: SeqNr) = Async.seqNr
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

    def foldWhile[S](from: SeqNr, s: S)(f: Fold[S, Event]) = {
      log[S](s"foldWhile from: $from, state: $s") {
        journal.foldWhile(from, s)(f)
      }
    }

    def lastSeqNr(from: SeqNr) = {
      log[SeqNr](s"lastSeqNr $from") {
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


  def apply(settings: Settings): Journal = ???


  // TODO create separate class IdAndTopic
  def apply(
    id: Id,
    topic: Topic,
    log: ActorLog, // TODO remove
    producer: Producer,
    newConsumer: () => Consumer[String, Bytes],
    eventual: EventualJournal,
    pollTimeout: FiniteDuration)(implicit
    ec: ExecutionContext): Journal = {

    val closeTimeout = 3.seconds // TODO from  config
    val withReadKafka = WithReadActions(newConsumer, pollTimeout, closeTimeout)

    val writeAction = WriteAction(id, topic, producer)

    apply(id, topic, log, eventual, withReadKafka, writeAction)
  }


  def apply(
    id: Id,
    topic: Topic,
    log: ActorLog,
    eventual: EventualJournal,
    withReadActions: WithReadActions,
    writeAction: WriteAction)(implicit
    ec: ExecutionContext): Journal = {

    def mark(): Async[Marker] = {
      val id = UUID.randomUUID().toString
      val action = Action.Mark(id)

      for {
        (partition, offset) <- writeAction(action)
      } yield {
        Marker(id, partition, offset)
      }
    }

    def foldActions(from: SeqNr): Async[FoldActions] = {
      val marker = mark()
      val topicPointers = eventual.topicPointers(topic)
      for {
        marker <- marker
        topicPointers <- topicPointers
      } yield {
        val offsetReplicated = topicPointers.pointers.get(marker.partition)

        // TODO compare partitions !
        val replicated = for {
          offset <- marker.offset
          offsetReplicated <- offsetReplicated
        } yield {
          offset.prev <= offsetReplicated
        }

        if (replicated getOrElse false) FoldActions.Empty
        else FoldActions(id, topic, from, marker, offsetReplicated, withReadActions)
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
      def foldWhile[S](from: SeqNr, s: S)(f: Fold[S, Event]) = {

        def replicatedSeqNr(from: SeqNr) = {
          val ss = (s, from, Option.empty[Offset])
          eventual.foldWhile(id, from, ss) { case ((s, _, _), replicated) =>
            val event = replicated.event
            val switch = f(s, event)
            val from = event.seqNr.next
            switch.map { s => (s, from, Some(replicated.partitionOffset.offset)) }
          }
        }

        def replicated(from: SeqNr) = {
          for {
            s <- eventual.foldWhile(id, from, s) { (s, replicated) => f(s, replicated.event) }
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
          foldActions <- foldActions(from)
          // TODO use range after eventualRecords
          // TODO prevent from reading calling consume twice!
          info <- foldActions(None, JournalInfo.empty) { (info, action) => info(action.header).continue }
          result <- info match {
            case JournalInfo.Empty                 => replicated(from)
            case JournalInfo.NonEmpty(_, deleteTo) => onNonEmpty(deleteTo, foldActions)
            case JournalInfo.DeleteTo(deleteTo)    => replicated(from max deleteTo.next)
          }
        } yield result
      }


      def lastSeqNr(from: SeqNr) = {
        for {
          foldActions <- foldActions(from)
          seqNrEventual = eventual.lastSeqNr(id, from)
          seqNr <- foldActions[SeqNr](None, from) { (seqNr, action) =>
            val result = action match {
              case action: Action.Append => action.header.range.to
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
        if (to <= 0) Async.unit
        else {
          val action = Action.Delete(to, timestamp)
          writeAction(action).unit
        }
      }

      override def toString = s"Journal($id)"
    }
  }
}