package com.evolutiongaming.kafka.journal

import java.time.Instant
import java.util.UUID

import akka.actor.ActorSystem
import com.evolutiongaming.kafka.journal.Alias._
import com.evolutiongaming.kafka.journal.FoldWhileHelper._
import com.evolutiongaming.kafka.journal.FutureHelper._
import com.evolutiongaming.kafka.journal.LogHelper._
import com.evolutiongaming.kafka.journal.eventual.{EventualJournal, PartitionOffset}
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.safeakka.actor.ActorLog
import com.evolutiongaming.skafka.consumer.Consumer
import com.evolutiongaming.skafka.producer.Producer
import com.evolutiongaming.skafka.{Bytes => _, _}

import scala.collection.immutable.Seq
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

// TODO consider passing topic along with id as method argument
// TODO should we return offset ?
trait Journal {
  def append(events: Nel[Event], timestamp: Instant): Future[Unit]
  // TODO decide on return type
  def read(range: SeqRange): Future[Seq[Event]]
  def lastSeqNr(from: SeqNr): Future[SeqNr]
  def delete(to: SeqNr, timestamp: Instant): Future[Unit]
}

object Journal {

  val Empty: Journal = new Journal {
    def append(events: Nel[Event], timestamp: Instant) = Future.unit
    def read(range: SeqRange): Future[List[Event]] = Future.nil
    def lastSeqNr(from: SeqNr) = Future.seqNr
    def delete(to: SeqNr, timestamp: Instant) = Future.unit

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

    def read(range: SeqRange) = {
      val toStr = (entries: Seq[Event]) => {
        entries.map(_.seqNr).mkString(",") // TODO use range and implement misses verification
      }

      log[Seq[Event]](s"read $range", toStr) {
        journal.read(range)
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
    system: ActorSystem,
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

    def mark(): Future[(String, Partition)] = {
      val marker = UUID.randomUUID().toString // TODO randomUUID ? overkill ?
      val action = Action.Mark(marker)

      for {
        partition <- writeAction(action)
      } yield {
        (marker, partition)
      }
    }

    trait Fold {
      // TODO f should return continue: Boolean
      def apply[S](s: S)(f: (S, Action.User) => S): Future[S]
    }

    // TODO add range argument
    val consumeActions = (from: SeqNr) => {
      val marker = mark()
      val topicPointers = eventual.topicPointers(topic)

      for {
        (marker, partition) <- marker
        topicPointers <- topicPointers
      } yield {
        val partitionOffset = for {
          offset <- topicPointers.pointers.get(partition)
        } yield {
          PartitionOffset(partition, offset)
        }
        // TODO compare partitions !

        new Fold {

          def apply[S](s: S)(f: (S, Action.User) => S): Future[S] = {

            withReadActions(topic, partitionOffset) { readActions =>

              val ff = (s: S) => {
                for {
                  actions <- readActions(id)
                } yield {
                  actions.foldWhile(s) {
                    case (s, action: Action.User) =>
                      val ss = f(s, action)
                      (ss, true)
                    case (s, action: Action.Mark) =>
                      val continue = action.header.id != marker
                      (s, continue)
                  }
                }
              }
              ff.foldWhile(s)
            }
          }
        }
      }
    }

    new Journal {

      def append(events: Nel[Event], timestamp: Instant): Future[Unit] = {

        val payload = EventsSerializer.toBytes(events)
        val range = SeqRange(from = events.head.seqNr, to = events.last.seqNr)
        val action = Action.Append(range, timestamp, payload)
        val result = writeAction(action)
        result.unit
      }

      def read(range: SeqRange): Future[Seq[Event]] = {

        def eventualRecords() = {

          val result = eventual.read(id, range.from, List.empty[Event]) { (events, record) =>
            val continue = record.seqNr <= range.to
            val event = Event(
              payload = record.payload,
              seqNr = record.seqNr,
              tags = record.tags)
            val result = if (continue) event :: events else events
            (result, continue)
          }

          for {
            (events, _) <- result
          } yield events.reverse
        }

        for {
          consume <- consumeActions(range.from)
          entries = eventualRecords()
          // TODO use range after eventualRecords

          // TODO prevent from reading calling consume twice!
          batch <- consume(ActionBatch.empty) { case (batch, action) =>
            batch(action.header)
          }

          result <- batch match {
            case ActionBatch.Empty => entries

            case ActionBatch.NonEmpty(lastSeqNr, deleteTo) =>

              def tmp(range: SeqRange) = {
                // TODO we don't need to consume if everything is deleted

                // TODO we don't need to consume if everything is in cassandra :)

                val result = consume(List.empty[Event]) { case (events, action) =>
                  action match {
                    case action: Action.Append =>
                      // TODO stop consuming

                      if (action.range intersects range) {
                        //                    val events = EventsSerializer.fromBytes(action.events)
                        // TODO add implicit method events.toList.foldWhile()

                        // TODO fix performance
                        val events2 =
                          EventsSerializer.fromBytes(action.events)
                            .toList
                            .filter { _.seqNr in range }

                        events ::: events2

                      } else {
                        // TODO stop consumption
                        events
                      }

                    case action: Action.Delete => events
                  }
                }

                for {
                  result <- result
                  entries <- entries
                } yield {
                  val entries2 = entries.filter(_.seqNr in range)

                  entries2.lastOption.fold(result) { last =>
                    entries2.toList ::: result.dropWhile(_.seqNr <= last.seqNr)
                  }
                }
              }

              deleteTo match {
                case None           => tmp(range)
                case Some(deleteTo) =>
                  if (range.to <= deleteTo) Future.nil
                  else {
                    val range2 =
                      if (range.from > deleteTo) range
                      else SeqRange(deleteTo.next, range.to) // TODO create separate method
                    tmp(range2)
                  }
              }


            case ActionBatch.DeleteTo(deleteTo) =>
              for {
                entries <- entries
              } yield {
                entries.dropWhile(_.seqNr <= deleteTo).toList
              }
          }
        } yield {
          result
        }
      }

      def lastSeqNr(from: SeqNr) = {
        for {
          consume <- consumeActions(from)
          seqNrEventual = eventual.lastSeqNr(id, from)
          seqNr <- consume[Offset](from) {
            case (seqNr, a: Action.Append) => a.header.range.to
            case (seqNr, _: Action.Delete) => seqNr
          }
          seqNrEventual <- seqNrEventual
        } yield {
          seqNrEventual max seqNr
        }
      }

      def delete(to: SeqNr, timestamp: Instant): Future[Unit] = {
        if (to <= 0) Future.unit
        else {
          val action = Action.Delete(to, timestamp)
          writeAction(action).unit
        }
      }

      override def toString = s"Journal($id)"
    }
  }
}