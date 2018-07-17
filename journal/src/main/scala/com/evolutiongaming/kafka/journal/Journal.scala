package com.evolutiongaming.kafka.journal

import java.time.Instant
import java.util.UUID

import com.evolutiongaming.concurrent.async.Async
import com.evolutiongaming.concurrent.async.AsyncConverters._
import com.evolutiongaming.kafka.journal.ActorLogHelper._
import com.evolutiongaming.kafka.journal.Alias._
import com.evolutiongaming.kafka.journal.AsyncHelper._
import com.evolutiongaming.kafka.journal.FoldWhileHelper._
import com.evolutiongaming.kafka.journal.eventual.{EventualJournal, PartitionOffset}
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
  def foldWhile[S](from: SeqNr, s: S)(f: Fold[S, Event]): Async[(S, Continue)]
  def lastSeqNr(from: SeqNr): Async[SeqNr]
  def delete(to: SeqNr, timestamp: Instant): Async[Unit]
}

object Journal {

  val Empty: Journal = new Journal {
    def append(events: Nel[Event], timestamp: Instant) = Async.unit
    def foldWhile[S](from: SeqNr, s: S)(f: Fold[S, Event]) = (s, true).async
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
      log[(S, Continue)](s"foldWhile from: $from, state: $s") {
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

    def mark(): Async[(String, Partition, Option[Offset])] = {
      val marker = UUID.randomUUID().toString // TODO randomUUID ? overkill ?
      val action = Action.Mark(marker)

      for {
        (partition, offset) <- writeAction(action)
      } yield {
        (marker, partition, offset)
      }
    }

    trait FoldActions {
      def apply[S](offset: Option[Offset], s: S)(f: Fold[S, Action.User]): Async[(S, Continue)]
    }

    object FoldActions {

      val Empty: FoldActions = new FoldActions {
        def apply[S](offset: Option[Offset], s: S)(f: Fold[S, Action.User]) = (s, true).async
      }


      // TODO add range argument
      def apply(from: SeqNr): Async[FoldActions] = {
        val marker = mark()
        val topicPointers = eventual.topicPointers(topic)

        for {
          (marker, partition, offsetLast) <- marker
          topicPointers <- topicPointers
        } yield {
          val offsetEventual = topicPointers.pointers.get(partition)

          // TODO compare partitions !
          val replicated = for {
            offsetLast <- offsetLast
            offsetReplicated <- offsetEventual
          } yield {
            offsetLast.prev <= offsetReplicated
          }

          if (replicated getOrElse false) {
            println(">>>>>>>>>>>>>>> MIRACLE 1 <<<<<<<<<<<<<<<")
            Empty
          } else {
            new FoldActions {

              def apply[S](offset: Option[Offset], s: S)(f: Fold[S, Action.User]) = {

                val replicated = for {
                  offsetLast <- offsetLast
                  offset <- offset
                } yield {
                  offsetLast.prev <= offset
                }

                if (replicated getOrElse false) {
                  println(">>>>>>>>>>>>>>> MIRACLE 2 <<<<<<<<<<<<<<<")
                  (s, true).async
                } else {
                  val partitionOffset = {
                    val offsetMax = PartialFunction.condOpt((offset, offsetEventual)) {
                      case (Some(o1), Some(o2)) => o1 max o2
                      case (Some(o), None)      => o
                      case (None, Some(o))      => o
                    }

                    for {offset <- offsetMax} yield PartitionOffset(partition, offset)
                  }

                  withReadActions(topic, partitionOffset) { readActions =>

                    val ff = (s: (S, Continue)) => {
                      for {
                        actions <- readActions(id)
                      } yield {

                        def apply(sb: (S, Continue), action: Action.User) = {
                          val (s, _) = sb
                          val result = f(s, action)
                          val (_, continue) = result
                          (result, continue)
                        }

                        // TODO verify we did not miss Mark and not cycled infinitely
                        actions.foldWhile(s) {
                          case (sb, action: Action.Append) => if (action.range.to < from) (sb, true) else apply(sb, action)
                          case (sb, action: Action.Delete) => apply(sb, action)
                          case (sb, action: Action.Mark)   => (sb, action.header.id != marker)
                        }
                      }
                    }
                    ff.foldWhile((s, true))
                  }
                }
              }
            }
          }
        }
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
            val (ss, continue) = f(s, event)
            val from = event.seqNr.next
            ((ss, from, Some(replicated.partitionOffset.offset)), continue)
          }
        }

        def replicated(from: SeqNr) = {
          eventual.foldWhile(id, from, s) { (s, replicated) => f(s, replicated.event) }
        }

        def onNonEmpty(deleteTo: Option[SeqNr], foldActions: FoldActions) = {

          def events(from: SeqNr, offset: Option[Offset], s: S) = {
            foldActions(offset, s) { case (s, action) =>
              action match {
                case action: Action.Append =>
                  if (action.range.to < from) {
                    (s, true)
                  } else {
                    val events = EventsSerializer.fromBytes(action.events)
                    events.foldWhile(s) { case (s, event) =>
                      if (event.seqNr >= from) f(s, event) else (s, true)
                    }
                  }

                case action: Action.Delete => (s, true)
              }
            }
          }


          val fromFixed = deleteTo.fold(from) { deleteTo => from max deleteTo.next }

          for {
            ((s, from, offset), continue) <- replicatedSeqNr(fromFixed)
            s <- if (continue) events(from, offset, s) else (s, continue).async
          } yield {
            s
          }
        }

        for {
          foldActions <- FoldActions(from)
          // TODO use range after eventualRecords
          // TODO prevent from reading calling consume twice!
          (batch, _) <- foldActions(None, ActionBatch.empty) { (batch, action) => (batch(action.header), true) }
          result <- batch match {
            case ActionBatch.Empty                         => replicated(from)
            case ActionBatch.NonEmpty(lastSeqNr, deleteTo) => onNonEmpty(deleteTo, foldActions)
            case ActionBatch.DeleteTo(deleteTo)            => replicated(from max deleteTo.next)
          }
        } yield result
      }


      def lastSeqNr(from: SeqNr) = {
        for {
          foldActions <- FoldActions(from)
          seqNrEventual = eventual.lastSeqNr(id, from)
          (seqNr, _) <- foldActions[SeqNr](None, from) { (seqNr, action) =>
            val result = action match {
              case action: Action.Append => action.header.range.to
              case action: Action.Delete => seqNr
            }
            (result, true)
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