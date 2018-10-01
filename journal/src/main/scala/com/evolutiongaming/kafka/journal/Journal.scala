package com.evolutiongaming.kafka.journal

import java.time.Instant
import java.util.UUID

import akka.actor.ActorSystem
import com.evolutiongaming.concurrent.async.Async
import com.evolutiongaming.concurrent.async.AsyncConverters._
import com.evolutiongaming.kafka.journal.AsyncHelper._
import com.evolutiongaming.kafka.journal.EventsSerializer._
import com.evolutiongaming.kafka.journal.FoldWhileHelper._
import com.evolutiongaming.kafka.journal.SeqNr.Helper._
import com.evolutiongaming.kafka.journal.eventual.EventualJournal
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.safeakka.actor.ActorLog
import com.evolutiongaming.skafka.producer.Producer
import com.evolutiongaming.skafka.{Bytes => _, _}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

trait Journal {

  def append(key: Key, events: Nel[Event], timestamp: Instant): Async[PartitionOffset]

  def read[S](key: Key, from: SeqNr, s: S)(f: Fold[S, Event]): Async[S]

  def lastSeqNr(key: Key, from: SeqNr): Async[Option[SeqNr]]

  // TODO return Pointer and Test it
  def delete(key: Key, to: SeqNr, timestamp: Instant): Async[Option[PartitionOffset]]
}

object Journal {

  val Empty: Journal = new Journal {

    def append(key: Key, events: Nel[Event], timestamp: Instant) = Async(PartitionOffset.Empty)

    def read[S](key: Key, from: SeqNr, s: S)(f: Fold[S, Event]) = s.async

    def lastSeqNr(key: Key, from: SeqNr) = Async.none

    def delete(key: Key, to: SeqNr, timestamp: Instant) = Async.none
  }


  def apply(journal: Journal, log: ActorLog): Journal = new Journal {

    def append(key: Key, events: Nel[Event], timestamp: Instant) = {
      for {
        tuple <- Latency { journal.append(key, events, timestamp) }
        (result, latency) = tuple
        _ = log.debug {
          val head = events.head.seqNr
          val last = events.last.seqNr
          val range = SeqRange(head, last)
          s"$key append in ${ latency }ms, events: $range, timestamp: $timestamp, result: $result"
        }
      } yield result
    }

    def read[S](key: Key, from: SeqNr, s: S)(f: Fold[S, Event]) = {
      for {
        tuple <- Latency { journal.read(key, from, s)(f) }
        (result, latency) = tuple
        _ = log.debug(s"$key read in ${ latency }ms, from: $from, state: $s, result: $result")
      } yield result
    }

    def lastSeqNr(key: Key, from: SeqNr) = {
      for {
        tuple <- Latency { journal.lastSeqNr(key, from) }
        (result, latency) = tuple
        _ = log.debug(s"$key lastSeqNr in ${ latency }ms, from: $from, result: $result")
      } yield result
    }

    def delete(key: Key, to: SeqNr, timestamp: Instant) = {
      for {
        tuple <- Latency { journal.delete(key, to, timestamp) }
        (result, latency) = tuple
        _ = log.debug(s"$key delete in ${ latency }ms, to: $to, timestamp: $timestamp, result: $result")
      } yield result
    }

    override def toString = journal.toString
  }


  def apply(journal: Journal, metrics: Metrics[Async]): Journal = new Journal {

    def append(key: Key, events: Nel[Event], timestamp: Instant) = {
      for {
        tuple <- Latency { journal.append(key, events, timestamp) }
        (result, latency) = tuple
        _ <- metrics.append(topic = key.topic, latency = latency, events = events.size)
      } yield result
    }

    def read[S](key: Key, from: SeqNr, s: S)(f: Fold[S, Event]) = {
      val ff: Fold[(S, Int), Event] = {
        case ((s, n), e) => f(s, e).map { s => (s, n + 1) }
      }
      for {
        tuple <- Latency { journal.read(key, from, (s, 0))(ff) }
        ((result, events), latency) = tuple
        _ <- metrics.read(topic = key.topic, latency = latency, events = events)
      } yield result
    }

    def lastSeqNr(key: Key, from: SeqNr) = {
      for {
        tuple <- Latency { journal.lastSeqNr(key, from) }
        (result, latency) = tuple
        _ <- metrics.pointer(key.topic, latency)
      } yield result
    }

    def delete(key: Key, to: SeqNr, timestamp: Instant) = {
      for {
        tuple <- Latency { journal.delete(key, to, timestamp) }
        (result, latency) = tuple
        _ <- metrics.delete(key.topic, latency)
      } yield result
    }
  }


  def apply(
    producer: Producer,
    origin: Option[Origin],
    topicConsumer: TopicConsumer,
    eventual: EventualJournal,
    pollTimeout: FiniteDuration = 100.millis,
    closeTimeout: FiniteDuration = 10.seconds)(implicit
    system: ActorSystem,
    ec: ExecutionContext): Journal = {

    val log = ActorLog(system, classOf[Journal])
    val journal = apply(log, origin, producer, topicConsumer, eventual, pollTimeout, closeTimeout)
    Journal(journal, log)
  }

  def apply(
    log: ActorLog, // TODO remove
    origin: Option[Origin],
    producer: Producer,
    topicConsumer: TopicConsumer,
    eventual: EventualJournal,
    pollTimeout: FiniteDuration,
    closeTimeout: FiniteDuration)(implicit
    ec: ExecutionContext): Journal = {

    val withReadActions = WithReadActions(topicConsumer, pollTimeout, closeTimeout, log)

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

    def readActions(key: Key, from: SeqNr): Async[FoldActions] = {
      val marker = {
        val id = UUID.randomUUID().toString
        val action = Action.Mark(key, Instant.now(), origin, id)
        for {
          partitionOffset <- appendAction(action)
        } yield {
          Marker(id, partitionOffset)
        }
      }
      val pointers = eventual.pointers(key.topic)
      for {
        marker <- marker
        pointers <- pointers
      } yield {
        val offset = pointers.values.get(marker.partition)
        FoldActions(key, from, marker, offset, withReadActions)
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
            _ = log.debug(s"$key read from: $from, offset: $offset")
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
          _ = log.debug(s"$key read info: $info")
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
          pointer = eventual.pointer(key, from)
          seqNr <- readActions(None /*TODO provide offset from eventual.lastSeqNr*/ , Option.empty[SeqNr]) { (seqNr, action) =>
            val result = action match {
              case action: Action.Append => Some(action.range.to)
              case action: Action.Delete => Some(action.to max seqNr)
            }
            result.continue
          }
          pointer <- pointer
        } yield {
          pointer.map(_.seqNr) max seqNr
        }
      }

      def delete(key: Key, to: SeqNr, timestamp: Instant) = {
        for {
          seqNr <- lastSeqNr(key, SeqNr.Min)
          result <- seqNr match {
            case None        => Async.none
            case Some(seqNr) =>

              // TODO not delete already delete, do not accept deleteTo=2 when already deleteTo=3
              val deleteTo = seqNr min to
              val action = Action.Delete(key, timestamp, origin, deleteTo)
              appendAction(action).map(Some(_))
          }
        } yield result
      }
    }
  }


  trait Metrics[F[_]] {

    def append(topic: Topic, latency: Long, events: Int): F[Unit]

    def read(topic: Topic, latency: Long, events: Int): F[Unit]

    def pointer(topic: Topic, latency: Long): F[Unit]

    def delete(topic: Topic, latency: Long): F[Unit]
  }

  object Metrics {

    def empty[F[_]](unit: F[Unit]): Metrics[F] = new Metrics[F] {

      def append(topic: Topic, latency: Long, events: Int) = unit

      def read(topic: Topic, latency: Long, events: Int) = unit

      def pointer(topic: Topic, latency: Long) = unit

      def delete(topic: Topic, latency: Long) = unit
    }
  }
}