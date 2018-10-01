package com.evolutiongaming.kafka.journal.replicator

import java.time.Instant

import com.evolutiongaming.kafka.journal.EventsSerializer._
import com.evolutiongaming.kafka.journal.FoldWhileHelper._
import com.evolutiongaming.kafka.journal.IO.Implicits._
import com.evolutiongaming.kafka.journal.KafkaConverters._
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.kafka.journal.eventual._
import com.evolutiongaming.kafka.journal.replicator.InstantHelper._
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.skafka.consumer._
import com.evolutiongaming.skafka.{Bytes => _, _}


// TODO partition replicator ?
// TODO add metric to track replication lag in case it cannot catchup with producers
// TODO verify that first consumed offset matches to the one expected, otherwise we fucked up.
trait TopicReplicator[F[_]] {
  def shutdown(): F[Unit]
}

// TODO add Mark latency metric

object TopicReplicator {

  //  TODO return error in case failed to connect
  def apply[F[_] : IO](
    topic: Topic,
    partitions: Set[Partition],
    consumer: KafkaConsumer[F],
    journal: ReplicatedJournal[F],
    log: Log[F],
    stopRef: Ref[Boolean, F],
    metrics: Metrics[F],
    now: => F[Instant] /*TODO should not be a function*/): TopicReplicator[F] = {

    def round(
      state: State,
      consumerRecords: ConsumerRecords[Id, Bytes],
      roundStart: Instant): F[(State, Map[TopicPartition, OffsetAndMetadata])] = {

      val records = for {
        records <- consumerRecords.values.values.toList
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
              val range =
                if (events.tail.isEmpty) events.head.seqNr.toString
                else s"${ events.head.seqNr }..${ events.last.seqNr }"
              val origin = records.head.action.origin
              val originStr = origin.fold("") { origin => s", origin: $origin" }
              s"append in ${ latency }ms, id: $id, offset: $partitionOffset, events: $range$originStr"
            }
          } yield {}
        }

        Batch.list(records).foldLeft(unit) { (result, batch) =>
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
          (topicPartition, records) <- consumerRecords.values
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
          if (pointersNew.values.isEmpty) unit
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
        state <- if (stop) state.stop.pure
        else for {
          roundStart <- now
          records <- consumer.poll() // TODO add kafka metrics
          stop <- stopRef.get()
          state <- {
            if (stop) state.stop.pure
            else if (records.values.isEmpty) state.continue.pure
            else for {
              timestamp <- now
              stateAndOffsets <- round(state, records, timestamp)
              (state, offsets) = stateAndOffsets
              _ <- consumer.commit(offsets)
              roundEnd <- now
              _ <- metrics.round(
                latency = roundEnd - roundStart,
                records = records.values.foldLeft(0) { case (acc, (_, record)) => acc + record.size })
            } yield state.continue
          }
        } yield state
      } yield state
    }

    val result = for {
      pointers <- journal.pointers(topic)
      partitionOffsets = for {
        partition <- partitions.toList
      } yield {
        val offset = pointers.values.getOrElse(partition, Offset.Min /*TODO is it correct?*/)
        PartitionOffset(partition = partition, offset = offset)
      }

      // TODO verify it started processing from right position

      _ <- consumer.subscribe(topic)
      _ <- IO[F].foldWhile(State.Empty)(consume)
    } yield {}

    // TODO rename
    val result2 = result.catchAll { failure =>
      failure.printStackTrace() // TODO
      log.error(s"failed: $failure", failure)
    }

    new TopicReplicator[F] {

      def shutdown() = {
        for {
          _ <- log.debug("shutting down")
          _ <- stopRef.set(true)
          _ <- result2
          _ <- consumer.close()
        } yield {}
      }

      override def toString = s"TopicReplicator($topic)"
    }
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


