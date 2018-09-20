package com.evolutiongaming.kafka.journal.replicator

import java.time.Instant

import com.evolutiongaming.kafka.journal.EventsSerializer._
import com.evolutiongaming.kafka.journal.FoldWhileHelper._
import com.evolutiongaming.kafka.journal.Implicits._
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
        records <- consumerRecords.values.values
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
        val partition = head.partitionOffset.partition
        val offsetLast = records.last.partitionOffset

        val id = key.id

        def measurements = for {
          now <- now
        } yield {
          Metrics.Measurements(
            partition = partition,
            replicationLatency = now - head.action.timestamp,
            deliveryLatency = roundStart - head.action.timestamp,
            actions = records.size)
        }

        def delete(deleteTo: SeqNr, bound: Boolean) = {
          for {
            _ <- journal.delete(key, roundStart, deleteTo, bound)
            measurements <- measurements
            latency = measurements.replicationLatency
            _ <- metrics.delete(measurements)
            _ <- log.info(s"delete in ${ latency }ms id: $id, deleteTo: $deleteTo, bound: $bound, offset: $offsetLast")
          } yield {}
        }

        def append(deleteTo: Option[SeqNr], events: Nel[ReplicatedEvent], bytes: => Int) = {
          for {
            _ <- journal.append(key, roundStart, events, deleteTo)
            measurements <- measurements
            _ <- metrics.append(
              events = events.length,
              bytes = bytes,
              measurements = measurements)
            latency = measurements.replicationLatency
            _ <- log.info {
              val deleteToStr = deleteTo.fold("") { deleteTo => s", deleteTo: $deleteTo" }
              val range = events.head.seqNr to events.last.seqNr
              s"append in ${ latency }ms id: $id, events: $range$deleteToStr, offset: $offsetLast"
            }
          } yield {}
        }

        def onNonEmpty(info: JournalInfo.NonEmpty) = {
          val deleteTo = info.deleteTo

          val appends = for {
            record <- records
            action <- PartialFunction.condOpt(record.action) { case a: Action.Append => a }.toIterable
            if deleteTo.forall(action.range.to > _)
          } yield {
            (record, action)
          }

          def bytes = appends.foldLeft(0) { case (bytes, (_, action)) =>
            bytes + action.payload.value.length /*TODO provide Bytes.length func */
          }

          val events = for {
            (record, action) <- appends
            event <- EventsFromPayload(action.payload, action.payloadType).toList
            if deleteTo.forall(event.seqNr > _)
          } yield {
            ReplicatedEvent(
              event = event,
              timestamp = action.timestamp,
              partitionOffset = record.partitionOffset,
              origin = action.origin)
          }

          Nel.opt(events) match {
            case Some(events) => append(info.deleteTo, events, bytes)
            case None         => info.deleteTo match {
              case Some(deleteTo) => delete(deleteTo, bound = true)
              case None           => unit
            }
          }
        }

        val info = records.foldLeft(JournalInfo.empty) { case (info, record) => info(record.action) }
        info match {
          case info: JournalInfo.NonEmpty => onNonEmpty(info)
          case info: JournalInfo.Deleted  => delete(info.deleteTo, bound = false)
          case JournalInfo.Empty          => unit
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
                duration = roundEnd - roundStart,
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

    def round(duration: Long, records: Int): F[Unit]
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
      actions: Int)
  }
}


