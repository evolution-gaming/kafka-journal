package com.evolutiongaming.kafka.journal

import java.util.UUID

import akka.actor.ActorSystem
import com.evolutiongaming.kafka.journal.ActionConverters._
import com.evolutiongaming.kafka.journal.Alias._
import com.evolutiongaming.kafka.journal.ConsumerHelper._
import com.evolutiongaming.kafka.journal.EventsSerializer._
import com.evolutiongaming.kafka.journal.FutureHelper._
import com.evolutiongaming.kafka.journal.eventual.{Eventual, PartitionOffset}
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.skafka.consumer.{Consumer, ConsumerRecord}
import com.evolutiongaming.skafka.producer.{Producer, ProducerRecord}
import com.evolutiongaming.skafka.{Offset, Partition, ToBytes, TopicPartition}

import scala.collection.immutable.Seq
import scala.compat.Platform
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

// TODO consider passing topic along with id as method argument
trait Client {
  def append(id: Id, events: Nel[Entry]): Future[Unit]
  // TODO decide on return type
  def read(id: Id, range: SeqRange): Future[Seq[Entry]]
  def lastSeqNr(id: Id, from: SeqNr): Future[SeqNr]
  def truncate(id: Id, to: SeqNr): Future[Unit]
}

object Client {

  val Empty: Client = new Client {
    def append(id: Id, events: Nel[Entry]) = Future.unit
    def read(id: Id, range: SeqRange): Future[List[Entry]] = Future.successful(Nil)
    def lastSeqNr(id: Id, from: SeqNr) = Future.successful(0L)
    def truncate(id: Id, to: SeqNr) = Future.unit
  }

  def apply(settings: Settings): Client = ???

  def apply(
    producer: Producer,
    newConsumer: () => Consumer[String, Bytes],
    eventual: Eventual = Eventual.Empty,
    pollTimeout: FiniteDuration = 100.millis)(implicit
    system: ActorSystem,
    ec: ExecutionContext): Client = {

    def toTopic(id: Id) = "journal"

    def produce[T](id: Id, action: Action, payload: T)(implicit toBytes: ToBytes[T]) = {
      val topic = toTopic(id)
      val header = toHeader(action)
      val timestamp = Platform.currentTime // TODO argument
      val record = ProducerRecord(
        topic = topic,
        value = payload,
        key = Some(id),
        timestamp = Some(timestamp),
        headers = List(header))
      producer(record)
    }

    def mark(id: Id): Future[(String, Partition)] = {
      val marker = UUID.randomUUID().toString
      val action = Action.Mark(marker)
      for {
        metadata <- produce(id, action, Array.empty[Byte])
      } yield {
        val partition = metadata.topicPartition.partition
        (marker, partition)
      }
    }

    def consume[S](
      id: Id,
      s: S,
      partitionOffset: Option[PartitionOffset])(
      f: (S, ConsumerRecord[String, Bytes]) => (S, Boolean)): Future[S] = {

      val consumer = newConsumer()

      val topic = toTopic(id)

      partitionOffset match {
        case None =>
          val topics = List(topic)
          consumer.subscribe(topics) // TODO with listener
        //          consumer.seekToBeginning()

        case Some(partitionOffset) =>
          val topicPartition = TopicPartition(topic, partitionOffset.partition)
          consumer.assign(List(topicPartition)) // TODO blocking
          consumer.seek(topicPartition, partitionOffset.offset) // TODO blocking
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
              val topic = record.topic
              // TODO important performance indication
              println(s"$id Client skipping topic: $topic, key: $key, offset: $offset, partition: $partition ")
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
      id: Id,
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
      def apply[S](s: S)(f: (S, ConsumerRecord[String, Bytes], Action.AppendOrTruncate) => S): Future[S]
    }

    val consumeActions = (id: Id, from: SeqNr) => {
      val marker = mark(id)
      val pointer = eventual.pointerOld(id, from)
      val topic = toTopic(id)
      val topicPointers = eventual.topicPointers(topic)

      for {
        (marker, partition) <- marker
        pointer <- pointer
        topicPointers <- topicPointers
        partitionOffsetOld = pointer.map { _.partitionOffset } // TODO
      } yield {
        val partitionOffset = for {
          offset <- topicPointers.pointers.get(partition)
        } yield {
          PartitionOffset(partition, offset)
        }

        println(s"$id partition: $partition, offset: ${ partitionOffset.map { _.offset } }, offset: ${ partitionOffsetOld.map { _.offset } }")

        // TODO compare partitions !

        new Fold {
          def apply[S](s: S)(f: (S, ConsumerRecord[String, Bytes], Action.AppendOrTruncate) => S): Future[S] = {

            // TODO add seqNr safety check
            consume(id, s, partitionOffsetOld) { case (s, record) =>
              val a = toAction(record)
              a match {
                case a: Action.AppendOrTruncate =>
                  val ss = f(s, record, a)
                  (ss, true)

                case a: Action.Mark =>
                  val continue = a.id != marker
                  (s, continue)
              }
            }
          }
        }
      }
    }

    new Client {

      def append(id: Id, events: Nel[Entry]): Future[Unit] = {

        val events2 = for {
          event <- events
        } yield {
          JournalRecord.Event(event.seqNr, event.payload)
        }

        val payload = JournalRecord.Payload.Events(events2)
        val range = SeqRange(from = events.head.seqNr, to = events.last.seqNr)
        val action = Action.Append(range)
        val result = produce(id, action, payload)
        result.unit
      }

      def read(id: Id, range: SeqRange): Future[Seq[Entry]] = {

        println(s"$id Client.read range: $range")

        val topic = toTopic(id) // TODO another topic created inside of consumeActions

        def eventualRecords() = {
          for {
            eventualRecords <- eventual.list(id, range)
          } yield {
            eventualRecords.map { record =>
              Entry(
                payload = record.payload,
                seqNr = record.seqNr,
                tags = record.tags)
            }
          }
        }

        case class Result(deleteTo: SeqNr, entries: Vector[Entry])

        val zero = Result(0, Vector.empty)

        val result = for {
          consume <- consumeActions(id, range.from)
          entries = eventualRecords()
          // TODO use range after eventualRecords
          records <- consume(zero) { case (result, record, a) =>
            a match {
              case action: Action.Append =>
                val bytes = record.value

                def entries = {
                  EventsFromBytes(bytes, topic)
                    .events
                    .to[Vector]
                    .map { event =>
                      val tags = Set.empty[String] // TODO
                      Entry(event.payload, event.seqNr, tags)
                    }
                }

                if (range.contains(action.range)) {
                  // TODO we don't need to deserialize entries that are out of scope
                  result.copy(entries = result.entries ++ entries)

                } else if (action.range < range) {
                  result
                } else if (action.range > range) {
                  // TODO stop consuming
                  result
                } else {

                  val filtered = entries.filter { entry => range contains entry.seqNr }

                  if (entries.last.seqNr > range) {
                    // TODO stop consuming
                    result.copy(entries = result.entries ++ filtered)
                  } else {
                    result.copy(entries = result.entries ++ filtered)
                  }
                }

              case a: Action.Truncate =>
                if (a.to > result.deleteTo) {
                  val entries = result.entries.dropWhile(_.seqNr <= a.to)
                  result.copy(deleteTo = a.to, entries = entries)
                } else {
                  result
                }
            }
          }
          entries <- entries
        } yield {

          val cassandraEntries = entries.dropWhile(_.seqNr <= records.deleteTo)

          cassandraEntries.lastOption match {
            case None =>
              // TODO important performance indication
              println(s"$id >>>> ${ records.entries.size } <<<<")
              records.entries

            case Some(last) =>
              val kafka = records.entries.dropWhile { _.seqNr <= last.seqNr }
              // TODO important performance indication
              println(s"$id >>>> ${ kafka.size }(${ cassandraEntries.size }) <<<<")
              cassandraEntries ++ kafka
          }
        }

        result.failed.foreach { failure =>
          failure.printStackTrace()
        }

        result
      }

      // TODO pass range
      def lastSeqNr(id: Id, from: SeqNr) = {
        val range = SeqRange(from = from)
        for {
          consume <- consumeActions(id, from)
          valueEventual = eventual.lastSeqNr(id, range)
          value <- consume[Offset](0L) { case (seqNr, _, a) =>
            a match {
              case a: Action.Append   => a.range.to
              case a: Action.Truncate => seqNr
            }
          }
        } yield {
          value
        }
      }

      def truncate(id: Id, to: SeqNr): Future[Unit] = {
        val action = Action.Truncate(to)
        produce(id, action, Array.empty[Byte]).unit
      }
    }
  }
}

// TODO timestamp ?
case class Entry(payload: Bytes, seqNr: SeqNr, tags: Set[String])
