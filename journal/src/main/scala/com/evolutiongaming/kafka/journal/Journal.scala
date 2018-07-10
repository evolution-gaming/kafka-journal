package com.evolutiongaming.kafka.journal

import java.time.Instant
import java.util.UUID

import akka.actor.ActorSystem
import com.evolutiongaming.kafka.journal.Alias._
import com.evolutiongaming.kafka.journal.ConsumerHelper._
import com.evolutiongaming.kafka.journal.FutureHelper._
import com.evolutiongaming.kafka.journal.KafkaConverters._
import com.evolutiongaming.kafka.journal.LogHelper._
import com.evolutiongaming.kafka.journal.eventual.{EventualJournal, PartitionOffset}
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.safeakka.actor.ActorLog
import com.evolutiongaming.skafka.consumer.{Consumer, ConsumerRecord}
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
    def read(range: SeqRange): Future[List[Event]] = Future.successful(Nil)
    def lastSeqNr(from: SeqNr) = Future.successful(0L)
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

    def consume[S](
      s: S,
      partitionOffset: Option[PartitionOffset])(
      f: (S, ConsumerRecord[String, Bytes]) => (S, Boolean)): Future[S] = {

      val consumer = newConsumer()

      partitionOffset match {
        case None =>
          val topics = List(topic)
          consumer.subscribe(topics) // TODO with listener
        //          consumer.seekToBeginning() // TODO

        case Some(partitionOffset) =>
          val topicPartition = TopicPartition(topic, partitionOffset.partition)
          consumer.assign(List(topicPartition)) // TODO blocking
        val offset = partitionOffset.offset + 1 // TODO TEST
          consumer.seek(topicPartition, offset) // TODO blocking
      }

      val ss = consumer.fold(s, pollTimeout) { (s, consumerRecords) =>
        // TODO check performance of flatten
        val records = consumerRecords.values.values.flatten
        val zero = (s, true)
        records.foldLeft(zero) { case (skip @ (s, continue), record) =>
          if (continue) {
            if (record.key contains id) f(s, record)
            else {
              val key = record.key getOrElse "none"
              val offset = record.offset
              val partition = record.partition
              // TODO important performance indication
              log.warn(s"skipping unnecessary record key: $key, partition: $partition, offset: $offset")
              skip
            }
          } else {
            skip
          }
        }
      }
      ss.onComplete { _ => consumer.close() } // TODO use timeout
      ss
    }


    // TODO case class Fold[S, T](state: S, f: () => ?) hm...

    /*def consumeStream[S, E](
      state: S,
      consumer: Consumer[String, Bytes])(
      f: (S, ConsumerRecord[String, Bytes]) => (Option[S], E)) = {

      consumer.source(state, pollTimeout) { (s, consumerRecords) =>

        // TODO check performance flatten and other places
        val records = consumerRecords.values.values.flatten.toVector
        val builder = Iterable.newBuilder[E]

        val ss = records.foldLeft[Option[S]](Some(s)) { (s, record) =>
          s.flatMap { s =>
            val (ss, e) = f(s, record)
            builder += e
            ss
          }
        }
        val es = builder.result()
        (ss, es)
      }
    }*/


    trait Fold {
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

            // TODO add seqNr safety check
            consume(s, partitionOffset) { case (s, record) =>
              val kafkaRecord = record.toKafkaRecord
              kafkaRecord.fold((s, true)) { kafkaRecord =>
                val action = kafkaRecord.action
                action match {
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
        }
      }
    }

    new Journal {

      def append(events: Nel[Event], timestamp: Instant): Future[Unit] = {

        val events2 = for {
          event <- events
        } yield {
          JournalRecord.Event(event.seqNr, event.payload)
        }
        val payload = EventsSerializer.EventsToBytes(JournalRecord.Payload.Events(events2), topic)
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

        val zero = Tmp.Result(SeqNr.Min, Vector.empty)

        for {
          consume <- consumeActions(range.from)
          entries = eventualRecords()
          // TODO use range after eventualRecords
          records <- consume(zero) { case (result, action) =>
            Tmp(result, action, topic, range)
          }
          entries <- entries
        } yield {

          val eventualEntries = entries.dropWhile(_.seqNr <= records.deleteTo)

          if (records.events.nonEmpty) {
            val size = records.events.size
            // TODO important performance indication
            // TODO decide on naming convention regarding records, entries, etc

            // TODO triggered by bug in consume that allows to consume deleted events
            log.warn(s"last $size records are missing in EventualJournal")
          }

          eventualEntries.lastOption.fold(records.events) { last =>
            val kafka = records.events.dropWhile(_.seqNr <= last.seqNr)
            // TODO create special data structure
            eventualEntries ++ kafka
          }
        }
      }

      def lastSeqNr(from: SeqNr) = {
        for {
          consume <- consumeActions(from)
          valueEventual = eventual.lastSeqNr(id, from)
          value <- consume[Offset](from) { case (seqNr, action) =>
            action match {
              case action: Action.Append => action.header.range.to
              case action: Action.Delete => seqNr
            }
          }
          valueEventual <- valueEventual
        } yield {

          val valueEventual2 = valueEventual getOrElse from
          value max valueEventual2
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