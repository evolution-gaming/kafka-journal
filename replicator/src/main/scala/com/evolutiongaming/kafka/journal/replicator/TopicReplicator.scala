package com.evolutiongaming.kafka.journal.replicator

import java.time.Instant

import cats.Applicative
import cats.effect.concurrent.Ref
import cats.effect.{Bracket, Clock, Concurrent, Sync}
import cats.implicits._
import com.evolutiongaming.kafka.journal.EventsSerializer._
import com.evolutiongaming.kafka.journal.KafkaConverters._
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.kafka.journal.eventual._
import com.evolutiongaming.kafka.journal.replicator.InstantHelper._
import com.evolutiongaming.kafka.journal.util.CatsHelper._
import com.evolutiongaming.kafka.journal.util.ClockHelper._
import com.evolutiongaming.kafka.journal.util.{FromFuture, Par}
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.skafka.consumer._
import com.evolutiongaming.skafka
import com.evolutiongaming.skafka.{Bytes => _, _}

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration


// TODO partition replicator ?
// TODO add metric to track replication lag in case it cannot catchup with producers
// TODO verify that first consumed offset matches to the one expected, otherwise we screwed.
trait TopicReplicator[F[_]] {

  def done: F[Unit]

  def close: F[Unit]
}

object TopicReplicator {

  //  TODO return error in case failed to connect
  def of[F[_] : Concurrent : Clock : Par : Metrics : ReplicatedJournal](
    topic: Topic,
    consumer: Consumer[F],
    stopRef: StopRef[F],
    log: Log[F]): F[TopicReplicator[F]] = {

    implicit val log1 = log
    implicit val consumer1 = consumer

    type State = TopicPointers

    def round(
      state: State,
      consumerRecords: Map[TopicPartition, List[ConsumerRecord[Id, Bytes]]],
      roundStart: Instant): F[(State, Map[TopicPartition, OffsetAndMetadata])] = {

      val records = for {
        records <- consumerRecords.values.toList
        record  <- records
        action  <- record.toAction
      } yield {
        val partitionOffset = PartitionOffset(record)
        ActionRecord(action, partitionOffset)
      }

      val ios = for {
        (key, records) <- records.groupBy(_.action.key)
      } yield {

        val head = records.head
        val id = key.id

        def measurements(records: Int) = {
          for {
            now <- Clock[F].instant
          } yield {
            Metrics.Measurements(
              partition = head.partition,
              replicationLatency = now - head.action.timestamp,
              deliveryLatency = roundStart - head.action.timestamp,
              records = records)
          }
        }

        def delete(partitionOffset: PartitionOffset, deleteTo: SeqNr, origin: Option[Origin]) = {
          for {
            _            <- ReplicatedJournal[F].delete(key, partitionOffset, roundStart, deleteTo, origin)
            measurements <- measurements(1)
            latency       = measurements.replicationLatency
            _            <- Metrics[F].delete(measurements)
            _            <- Log[F].info {
              val originStr = origin.fold("") { origin => s", origin: $origin" }
              s"delete in ${ latency }ms, id: $id, offset: $partitionOffset, deleteTo: $deleteTo$originStr"
            }
          } yield {}
        }

        def append(partitionOffset: PartitionOffset, records: Nel[ActionRecord[Action.Append]]) = {

          val bytes = records.foldLeft(0) { case (bytes, record) => bytes + record.action.payload.size }

          val events = for {
            record <- records
            action  = record.action
            event  <- EventsFromPayload(action.payload, action.payloadType)
          } yield {
            ReplicatedEvent(record, event)
          }

          for {
            _            <- ReplicatedJournal[F].append(key, partitionOffset, roundStart, events)
            measurements <- measurements(records.size)
            _            <- Metrics[F].append(
              events = events.length,
              bytes = bytes,
              measurements = measurements)
            latency       = measurements.replicationLatency
            _            <- Log[F].info {
              val seqNrs =
                if (events.tail.isEmpty) s"seqNr: ${ events.head.seqNr }"
                else s"seqNrs: ${ events.head.seqNr }..${ events.last.seqNr }"
              val origin = records.head.action.origin
              val originStr = origin.fold("") { origin => s", origin: $origin" }
              s"append in ${ latency }ms, id: $id, offset: $partitionOffset, $seqNrs$originStr"
            }
          } yield {}
        }

        Batch.list(records).foldLeft(().pure[F]) { (result, batch) =>
          for {
            _ <- result
            _ <- batch match {
              case batch: Batch.Appends => append(batch.partitionOffset, batch.records)
              case batch: Batch.Delete  => delete(batch.partitionOffset, batch.seqNr, batch.origin)
            }
          } yield {}
        }
      }

      val savePointers = {

        val offsets = for {
          (topicPartition, records) <- consumerRecords
          offset = records.foldLeft(Offset.Min) { (offset, record) => record.offset max offset }
        } yield {
          (topicPartition, offset)
        }

        val offsetsToCommit = for {
          (topicPartition, offset) <- offsets
        } yield {
          val offsetAndMetadata = OffsetAndMetadata(offset + 1)
          (topicPartition, offsetAndMetadata)
        }

        val pointersNew = TopicPointers {
          for {
            (topicPartition, offset) <- offsets
          } yield {
            (topicPartition.partition, offset)
          }
        }

        for {
          _ <- {
            if (pointersNew.values.isEmpty) ().pure[F]
            else ReplicatedJournal[F].save(topic, pointersNew, roundStart)
          }
        } yield {
          val stateNew = state + pointersNew
          (stateNew, offsetsToCommit)
        }
      }

      for {
        _        <- Par[F].unorderedFold(ios)
        pointers <- savePointers
      } yield pointers
    }

    def ifContinue(fa: F[Either[State, Unit]]) = {
      for {
        stop   <- stopRef.get
        result <- if (stop) ().asRight[State].pure[F] else fa
      } yield result
    }

    def consume(state: State): F[Either[State, Unit]] = {
      ifContinue {
        for {
          roundStart      <- Clock[F].instant
          consumerRecords <- Consumer[F].poll
          state           <- ifContinue {
            val records = for {
              (topicPartition, records) <- consumerRecords.values
              partition = topicPartition.partition
              offset = state.values.get(partition)
              result = offset.fold(records) { offset =>
                for {
                  record <- records
                  if record.offset > offset
                } yield record
              }
              if result.nonEmpty
            } yield {
              (topicPartition, result)
            }

            if (records.isEmpty) state.asLeft[Unit].pure[F]
            else for {
              timestamp        <- Clock[F].instant
              stateAndOffsets  <- round(state, records.toMap, timestamp)
              (state, offsets)  = stateAndOffsets
              _                <- Consumer[F].commit(offsets)
              roundEnd         <- Clock[F].instant
              _                <- Metrics[F].round(
                latency = roundEnd - roundStart,
                records = consumerRecords.values.foldLeft(0) { case (acc, (_, record)) => acc + record.size })
            } yield state.asLeft[Unit]
          }
        } yield state
      }
    }

    for {
      pointers <- ReplicatedJournal[F].pointers(topic)
      _        <- Consumer[F].subscribe(topic)
      fiber    <- Concurrent[F].start {
        val subscription = pointers
          .tailRecM(consume)
          .onError { case error => Log[F].error(s"failed with $error", error) /*TODO fail the app*/ }
        Bracket[F, Throwable].guarantee(subscription)(Consumer[F].close)
      }
    } yield {
      new TopicReplicator[F] {

        def done = fiber.join

        def close = {
          for {
            _ <- Log[F].debug("shutting down")
            _ <- stopRef.set
            _ <- fiber.join
          } yield {}
        }
      }
    }
  }

  def of[F[_] : Concurrent : Clock : Par : Metrics : ReplicatedJournal](
    topic: Topic,
    consumer: F[Consumer[F]]): F[TopicReplicator[F]] = {

    for {
      consumer <- consumer
      log      <- Log.of[F](TopicReplicator.getClass)
      topicLog  = log prefixed topic
      stopRef  <- StopRef.of[F]
      result   <- of[F](topic, consumer, stopRef, topicLog)
    } yield result
  }


  trait Consumer[F[_]] {

    def subscribe(topic: Topic): F[Unit]

    def poll: F[ConsumerRecords[Id, Bytes]]

    def commit(offsets: Map[TopicPartition, OffsetAndMetadata]): F[Unit]

    def close: F[Unit]
  }

  object Consumer {

    def apply[F[_]](implicit F: Consumer[F]): Consumer[F] = F

    def apply[F[_] : Sync : FromFuture](
      consumer: skafka.consumer.Consumer[Id, Bytes, Future],
      pollTimeout: FiniteDuration /*TODO*/): Consumer[F] = {

      new Consumer[F] {

        def subscribe(topic: Topic) = {
          Sync[F].delay {
            consumer.subscribe(Nel(topic), None)
          }
        }

        def poll = {
          FromFuture[F].apply {
            consumer.poll(pollTimeout)
          }
        }

        def commit(offsets: Map[TopicPartition, OffsetAndMetadata]) = {
          FromFuture[F].apply {
            consumer.commit(offsets)
          }
        }

        def close = {
          FromFuture[F].apply {
            consumer.close()
          }
        }
      }
    }
  }


  trait StopRef[F[_]] {

    def set: F[Unit]

    def get: F[Boolean]
  }

  object StopRef {

    def apply[F[_]](implicit F: StopRef[F]): StopRef[F] = F

    def of[F[_] : Sync]: F[StopRef[F]] = {
      for {
        ref <- Ref.of[F, Boolean](false)
      } yield {
        new StopRef[F] {
          def set = ref.set(true)
          def get = ref.get
        }
      }
    }
  }


  trait Metrics[F[_]] {
    import Metrics._

    // TODO add content type
    def append(events: Int, bytes: Int, measurements: Measurements): F[Unit]

    def delete(measurements: Measurements): F[Unit]

    def round(latency: Long, records: Int): F[Unit]
  }

  object Metrics {

    def apply[F[_]](implicit F: Metrics[F]): Metrics[F] = F

    def empty[F[_] : Applicative]: Metrics[F] = empty(Applicative[F].unit)

    def empty[F[_]](unit: F[Unit]): Metrics[F] = new Metrics[F] {

      def append(events: Int, bytes: Int, measurements: Measurements) = unit

      def delete(measurements: Measurements) = unit

      def round(duration: Long, records: Int) = unit
    }

    final case class Measurements(
      partition: Partition,
      replicationLatency: Long,
      deliveryLatency: Long,
      records: Int)
  }
}


