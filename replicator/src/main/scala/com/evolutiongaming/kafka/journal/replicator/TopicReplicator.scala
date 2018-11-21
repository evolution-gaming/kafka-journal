package com.evolutiongaming.kafka.journal.replicator

import java.time.Instant

import akka.actor.ActorSystem
import com.evolutiongaming.kafka.journal.EventsSerializer._
import com.evolutiongaming.kafka.journal.FoldWhile._
import com.evolutiongaming.kafka.journal.IO.ops._
import com.evolutiongaming.kafka.journal.KafkaConverters._
import com.evolutiongaming.kafka.journal.eventual._
import com.evolutiongaming.kafka.journal.replicator.InstantHelper._
import com.evolutiongaming.kafka.journal.{IO, _}
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.safeakka.actor.ActorLog
import com.evolutiongaming.skafka.consumer._
import com.evolutiongaming.skafka.{Bytes => _, _}


// TODO partition replicator ?
// TODO add metric to track replication lag in case it cannot catchup with producers
// TODO verify that first consumed offset matches to the one expected, otherwise we screwed.
trait TopicReplicator[F[_]] {

  def done(): F[Unit]

  def shutdown(): F[Unit]
}

// TODO add Mark latency metric

object TopicReplicator {

  //  TODO return error in case failed to connect
  def apply[F[_] : IO](
    topic: Topic,
    consumer: KafkaConsumer[F],
    journal: ReplicatedJournal[F],
    log: Log[F],
    stopRef: Ref[Boolean, F],
    metrics: Metrics[F],
    now: => F[Instant] /*TODO should not be a function*/): TopicReplicator[F] = {

    def round(
      state: State,
      consumerRecords: Map[TopicPartition, List[ConsumerRecord[Id, Bytes]]],
      roundStart: Instant): F[(State, Map[TopicPartition, OffsetAndMetadata])] = {

      val records = for {
        records <- consumerRecords.values.toList
        record <- records
        action <- record.toAction
      } yield {
        val partitionOffset = PartitionOffset(record)
        ActionRecord(action, partitionOffset)
      }

      val ios = for {
        (key, records) <- records.groupBy(_.action.key)
      } yield {

        val head = records.head
        val id = key.id

        def measurements(records: Int) = for {
          now <- now
        } yield {
          Metrics.Measurements(
            partition = head.partition,
            replicationLatency = now - head.action.timestamp,
            deliveryLatency = roundStart - head.action.timestamp,
            records = records)
        }

        def delete(partitionOffset: PartitionOffset, deleteTo: SeqNr, origin: Option[Origin]) = {
          for {
            _ <- journal.delete(key, partitionOffset, roundStart, deleteTo, origin)
            measurements <- measurements(1)
            latency = measurements.replicationLatency
            _ <- metrics.delete(measurements)
            _ <- log.info {
              val originStr = origin.fold("") { origin => s", origin: $origin" }
              s"delete in ${ latency }ms, id: $id, offset: $partitionOffset, deleteTo: $deleteTo$originStr"
            }
          } yield {}
        }

        def append(partitionOffset: PartitionOffset, records: Nel[ActionRecord[Action.Append]]) = {

          val bytes = records.foldLeft(0) { case (bytes, record) => bytes + record.action.payload.size }

          val events = for {
            record <- records
            action = record.action
            event <- EventsFromPayload(action.payload, action.payloadType)
          } yield {
            ReplicatedEvent(record, event)
          }

          for {
            _ <- journal.append(key, partitionOffset, roundStart, events)
            measurements <- measurements(records.size)
            _ <- metrics.append(
              events = events.length,
              bytes = bytes,
              measurements = measurements)
            latency = measurements.replicationLatency
            _ <- log.info {
              val seqNrs =
                if (events.tail.isEmpty) s"seqNr: ${ events.head.seqNr }"
                else s"seqNrs: ${ events.head.seqNr }..${ events.last.seqNr }"
              val origin = records.head.action.origin
              val originStr = origin.fold("") { origin => s", origin: $origin" }
              s"append in ${ latency }ms, id: $id, offset: $partitionOffset, $seqNrs$originStr"
            }
          } yield {}
        }

        Batch.list(records).foldLeft(IO[F].unit) { (result, batch) =>
          for {
            _ <- result
            _ <- batch match {
              case batch: Batch.Appends => append(batch.partitionOffset, batch.records)
              case batch: Batch.Delete  => delete(batch.partitionOffset, batch.seqNr, batch.origin)
            }
          } yield {}
        }
      }

      def savePointers() = {

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

        val result = {
          if (pointersNew.values.isEmpty) IO[F].unit
          else journal.save(topic, pointersNew, roundStart)
        }

        for {
          _ <- result
        } yield {
          val stateNew = state.copy(state.pointers + pointersNew)
          (stateNew, offsetsToCommit)
        }
      }

      for {
        _ <- IO[F].foldUnit(ios)
        pointers <- savePointers()
      } yield pointers
    }

    // TODO cache state and not re-read it when kafka is broken
    def consume(state: State): F[Switch[State]] = {
      for {
        stop <- stopRef.get()
        state <- {
          if (stop) IO[F].pure(state.stop)
          else for {
            roundStart <- now
            consumerRecords <- consumer.poll()
            stop <- stopRef.get()
            state <- {
              if (stop) IO[F].pure(state.stop)
              else {
                val records = for {
                  (topicPartition, records) <- consumerRecords.values
                  partition = topicPartition.partition
                  offset = state.pointers.values.get(partition)
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

                if (records.isEmpty) IO[F].pure(state.continue)
                else for {
                  timestamp <- now
                  stateAndOffsets <- round(state, records.toMap, timestamp)
                  (state, offsets) = stateAndOffsets
                  _ <- consumer.commit(offsets)
                  roundEnd <- now
                  _ <- metrics.round(
                    latency = roundEnd - roundStart,
                    records = consumerRecords.values.foldLeft(0) { case (acc, (_, record)) => acc + record.size })
                } yield state.continue
              }
            }
          } yield state
        }
      } yield state
    }

    val result = {
      val result = for {
        pointers <- journal.pointers(topic)
        _ <- consumer.subscribe(topic)
        _ <- IO[F].foldWhile1(State(pointers))(consume)
      } yield {}

      result.flatMapFailure { failure =>
        log.error(s"failed: $failure", failure)
      }
    }

    new TopicReplicator[F] {

      def done() = result

      def shutdown() = {
        for {
          _ <- log.debug("shutting down")
          _ <- stopRef.set(true)
          _ <- result
          _ <- consumer.close()
        } yield {}
      }

      override def toString = s"TopicReplicator($topic)"
    }
  }

  def apply[F[_]: IO](
    topic: Topic,
    journal: ReplicatedJournal[F],
    consumer: KafkaConsumer[F],
    metrics: Metrics[F])(implicit system: ActorSystem): TopicReplicator[F] = {

    val actorLog = ActorLog(system, TopicReplicator.getClass) prefixed topic
    val stopRef = Ref[Boolean, F]()
    apply(
      topic = topic,
      consumer = consumer,
      journal = journal,
      log = Log(actorLog),
      stopRef = stopRef,
      metrics = metrics,
      now = IO[F].point(Instant.now)/*TODO*/)
  }


  final case class State(pointers: TopicPointers = TopicPointers.Empty)

  object State {
    val Empty: State = State()
  }


  trait Metrics[F[_]] {
    import Metrics._

    // TODO add content type
    def append(events: Int, bytes: Int, measurements: Measurements): F[Unit]

    def delete(measurements: Measurements): F[Unit]

    def round(latency: Long, records: Int): F[Unit]
  }

  object Metrics {

    def empty[F[_]: IO]: Metrics[F] = new Metrics[F] {

      def append(events: Int, bytes: Int, measurements: Measurements) = IO[F].unit

      def delete(measurements: Measurements) = IO[F].unit

      def round(duration: Long, records: Int) = IO[F].unit
    }

    final case class Measurements(
      partition: Partition,
      replicationLatency: Long,
      deliveryLatency: Long,
      records: Int)
  }
}


