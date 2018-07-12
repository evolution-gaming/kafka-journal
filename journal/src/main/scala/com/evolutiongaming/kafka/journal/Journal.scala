package com.evolutiongaming.kafka.journal

import java.time.Instant
import java.util.UUID

import akka.actor.ActorSystem
import com.evolutiongaming.kafka.journal.Alias._
import com.evolutiongaming.kafka.journal.FoldWhileHelper._
import com.evolutiongaming.kafka.journal.FutureHelper._
import com.evolutiongaming.kafka.journal.KafkaConverters._
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
    val withReadKafka = WithReadKafka(newConsumer, pollTimeout, closeTimeout)

    apply(id, topic, log, producer, eventual, withReadKafka)
  }

  def apply(
    id: Id,
    topic: Topic,
    log: ActorLog, // TODO remove
    producer: Producer,
    eventual: EventualJournal,
    withReadKafka: WithReadKafka)(implicit
    system: ActorSystem,
    ec: ExecutionContext): Journal = {

    def produce(action: Action) = {
      val kafkaRecord = KafkaRecord(id, topic, action)
      val producerRecord = kafkaRecord.toProducerRecord
      producer(producerRecord)
    }

    def mark(): Future[(String, Partition)] = {
      val marker = UUID.randomUUID().toString
      val header = Action.Header.Mark(marker)
      val action = Action.Mark(header)

      for {
        metadata <- produce(action)
      } yield {
        val partition = metadata.topicPartition.partition
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

            withReadKafka(topic, partitionOffset) { readKafka =>

              val ff = (s: S) => {
                for {
                  records <- readKafka(id)
                } yield {
                  records.foldWhile(s) { (s, record) =>
                    record.action match {
                      case action: Action.User =>
                        val ss = f(s, action)
                        (ss, true)
                      case action: Action.Mark =>
                        val continue = action.header.id != marker
                        (s, continue)
                    }
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
        val header = Action.Header.Append(range)
        val action = Action.Append(header, timestamp, payload)
        val result = produce(action)
        result.unit
      }

      def read(range: SeqRange): Future[Seq[Event]] = {

        def eventualRecords() = {
          for {
            eventualRecords <- eventual.read(id, range)
          } yield {
            eventualRecords.map { record =>
              Event(
                payload = record.payload,
                seqNr = record.seqNr,
                tags = record.tags)
            }
          }
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
            case ActionBatch.Empty                         => entries
            case ActionBatch.NonEmpty(lastSeqNr, deleteTo) =>

              val deleteTo2 = deleteTo getOrElse SeqNr.Min

              // TODO we don't need to consume if everything is in cassandra :)

              val result = consume(List.empty[Event]) { case (events, action) =>
                action match {
                  case action: Action.Append =>

                    // TODO stop consuming
                    if(action.range.from > range || action.range.to <= deleteTo2) {
                      events
                    } else {
                      //                    val events = EventsSerializer.fromBytes(action.events)
                      // TODO add implicit method events.toList.foldWhile()

                      // TODO fix performance
                      val events2 =
                        EventsSerializer.fromBytes(action.events)
                          .toList
                          .takeWhile(_.seqNr <= range.to)
                          .takeWhile(_.seqNr < deleteTo2)

                      events ::: events2
                    }

                  case action: Action.Delete => events
                }
              }

              for {
                result <- result
                entries <- entries
              } yield {

                val entries2 = entries.dropWhile(_.seqNr <= deleteTo2)

                entries2.lastOption.fold(result) { last =>
                  entries2.toList ::: entries2.toList.dropWhile(_.seqNr <= last.seqNr)
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
        val header = Action.Header.Delete(to)
        val action = Action.Delete(header, timestamp)
        produce(action).unit
      }

      override def toString = s"Journal($id)"
    }
  }
}