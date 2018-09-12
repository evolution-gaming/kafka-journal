package com.evolutiongaming.kafka.journal.replicator

import java.time.Instant

import com.evolutiongaming.kafka.journal.FoldWhileHelper._
import com.evolutiongaming.kafka.journal.Implicits._
import com.evolutiongaming.kafka.journal.KafkaConverters._
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.kafka.journal.eventual._
import com.evolutiongaming.kafka.journal.replicator.InstantHelper._
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.skafka.consumer._
import com.evolutiongaming.skafka.{Bytes => _, _}

import scala.language.existentials


// TODO partition replicator ?
// TODO add metric to track replication lag in case it cannot catchup with producers
// TODO verify that first consumed offset matches to the one expected, otherwise we fucked up.
trait TopicReplicator[F[_]] {
  def shutdown(): F[Unit]
}

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
      consumerRecords: ConsumerRecords[String, Bytes],
      timestamp: Instant): F[(State, Map[TopicPartition, OffsetAndMetadata])] = {

      // TODO avoid creating unnecessary collections
      val records = for {
        consumerRecords <- consumerRecords.values.values
        consumerRecord <- consumerRecords
        kafkaRecord <- consumerRecord.toKafkaRecord
      } yield {
        val partitionOffset = PartitionOffset(
          partition = consumerRecord.partition,
          offset = consumerRecord.offset)
        (partitionOffset, kafkaRecord)
      }

      val ios = for {
        (key, records) <- records.groupBy { case (_, record) => record.key }
      } yield {

        val (partitionOffset, head) = records.head
        val offset = partitionOffset.offset
        val id = key.id

        def latency = for {
          now <- now
        } yield {
          // TODO not sure this is right to take `head` for this measurement, as it shows the worst case
          now - head.action.timestamp
        }

        def delete(deleteTo: SeqNr, bound: Boolean) = {
          for {
            _ <- journal.delete(key, timestamp, deleteTo, bound)
            latency <- latency
            _ <- metrics.delete(
              partition = partitionOffset.partition,
              latency = latency,
              records = records.size)
            _ <- log.info(s"delete in ${ latency }ms id: $id, deleteTo: $deleteTo, bound: $bound, offset: $offset")
          } yield {}
        }

        def append(deleteTo: Option[SeqNr], events: Nel[ReplicatedEvent]) = {
          for {
            _ <- journal.append(key, timestamp, events, deleteTo)
            latency <- latency
            _ <- metrics.append(
              partition = partitionOffset.partition,
              latency = latency,
              events = events.length,
              records = records.size,
              bytes = events.foldLeft(0)((bytes, event) => bytes + event.event.payload.value.length))
            _ <- log.info {
              val deleteToStr = deleteTo.fold("") { deleteTo => s", deleteTo: $deleteTo" }
              val range = events.head.seqNr to events.last.seqNr
              s"append in ${ latency }ms id: $id, range: $range$deleteToStr, offset: $offset"
            }
          } yield {}
        }

        def onNonEmpty(info: JournalInfo.NonEmpty) = {
          val deleteTo = info.deleteTo
          val events = for {
            (partitionOffset, record) <- records
            action <- PartialFunction.condOpt(record.action) { case a: Action.Append => a }.toIterable
            if deleteTo.forall(action.range.to > _)
            event <- EventsSerializer.fromBytes(action.events).toList
            if deleteTo.forall(event.seqNr > _)
          } yield {
            ReplicatedEvent(event, action.timestamp, partitionOffset)
          }

          Nel.opt(events) match {
            case Some(events) => append(info.deleteTo, events)
            case None         => info.deleteTo match {
              case Some(deleteTo) => delete(deleteTo, bound = true)
              case None           => unit
            }
          }
        }

        val info = records.foldLeft(JournalInfo.empty) { case (info, (_, record)) => info(record.action.header) }
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
          else journal.save(topic, pointersNew)
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
        val offset = pointers.values.getOrElse(partition, 0l /*TODO is it correct?*/)
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

  case class State(pointers: TopicPointers = TopicPointers.Empty)

  object State {
    val Empty: State = State()
  }


  trait Metrics[F[_]] {

    // TODO add content type
    def append(partition: Partition, latency: Long, events: Int, records: Int, bytes: Int): F[Unit]

    def delete(partition: Partition, latency: Long, records: Int): F[Unit]

    def round(duration: Long, records: Int): F[Unit]
  }

  object Metrics {

    def empty[F[_]](unit: F[Unit]): Metrics[F] = new Metrics[F] {

      def append(partition: Partition, latency: Long, events: Int, records: Int, bytes: Int) = unit

      def delete(partition: Partition, latency: Long, records: Int) = unit

      def round(duration: Long, records: Int) = unit
    }
  }
}


