package com.evolutiongaming.kafka.journal.replicator

import java.time.Instant

import com.evolutiongaming.kafka.journal.FoldWhileHelper._
import com.evolutiongaming.kafka.journal.Implicits._
import com.evolutiongaming.kafka.journal.KafkaConverters._
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.kafka.journal.eventual._
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.skafka.consumer._
import com.evolutiongaming.skafka.{Bytes => _, _}

import scala.compat.Platform
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
    stopRef: Ref[Boolean, F] /*,
    currentTime: F[Long]*/): TopicReplicator[F] = {

    // TODO handle that consumerRecords are not empty
    def apply(
      state: State,
      consumerRecords: ConsumerRecords[String, Bytes],
      timestamp: Instant): F[(State, Map[TopicPartition, OffsetAndMetadata])] = {

      // TODO avoid creating unnecessary collections
      val records = for {
        consumerRecords <- consumerRecords.values.values
        consumerRecord <- consumerRecords
        kafkaRecord <- consumerRecord.toKafkaRecord
        // TODO kafkaRecord.asInstanceOf[Action.User] ???
      } yield {
        val partitionOffset = PartitionOffset(
          partition = consumerRecord.partition,
          offset = consumerRecord.offset)
        (kafkaRecord, partitionOffset)
      }

      val ios = for {
        (key, records) <- records.groupBy { case (record, _) => record.key }
      } yield {

        val (last, partitionOffset) = records.last
        val offset = partitionOffset.offset
        val id = key.id

        def measureLatency = {
          val time = Platform.currentTime
          time - last.action.timestamp.toEpochMilli
        }

        def delete(deleteTo: SeqNr, bound: Boolean) = {
          for {
            _ <- journal.delete(key, timestamp, deleteTo, bound)
            latency = measureLatency
            _ <- log.info(s"delete $id in ${ latency }ms, deleteTo: $deleteTo, bound: $bound, offset: $offset")
          } yield {}
        }

        def append(deleteTo: Option[SeqNr], events: Nel[ReplicatedEvent]) = {
          for {
            _ <- journal.append(key, timestamp, events, deleteTo)
            latency = measureLatency
            _ <- log.info {
              val range = events.head.seqNr to events.last.seqNr
              s"append $id in ${ latency }ms, range: $range deleteTo: $deleteTo, offset: $offset"
            }
          } yield {}
        }

        def onNonEmpty(info: JournalInfo.NonEmpty) = {
          val deleteTo = info.deleteTo
          val events = for {
            (record, partitionOffset) <- records
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

        val info = records.foldLeft(JournalInfo.empty) { case (info, (record, _)) => info(record.action.header) }
        info match {
          case info: JournalInfo.NonEmpty => onNonEmpty(info)
          case info: JournalInfo.Deleted  => delete(info.deleteTo, bound = false)
          case JournalInfo.Empty          => unit
        }
      }

      def savePointers() = {

        val offsets = for {
          (topicPartition, records) <- consumerRecords.values
          offset = records.foldLeft(0l /*TODO Offset.Min*/) { (offset, record) => record.offset max offset }
          if offset != 0l /*TODO*/
        } yield {
          (topicPartition, offset)
        }

        val offsetsToCommit = for {
          (topicPartition, offset) <- offsets
        } yield {
          val offsetAndMetadata = OffsetAndMetadata(offset + 1, "" /* TODO*/)
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
    def consume(state: State) = {
      IO[F].foldWhile(state) { state =>
        for {
          stop <- stopRef.get()
          state <- if (stop) state.stop.pure
          else for {
            records <- consumer.poll()
            stop <- stopRef.get()
            state <- {
              if (stop) state.stop.pure
              else if (records.values.isEmpty) state.continue.pure
              else for {
                stateAndOffsets <- apply(state, records, /*TODO*/ Instant.now())
                (state, offsets) = stateAndOffsets
                _ <- consumer.commit(offsets)
              } yield state.continue
            }
          } yield state
        } yield state
      }
    }

    val result = for {
      //      _ <- consumer.subscribe(topic)
      // TODO seek to the beginning
      // TODO acknowledge ?
      pointers <- journal.pointers(topic)
      partitionOffsets = for {
        partition <- partitions.toList
      } yield {
        val offset = pointers.values.getOrElse(partition, 0l /*TODO is it correct?*/)
        PartitionOffset(partition = partition, offset = offset)
      }

      // TODO verify it started processing from right position

      _ <- consumer.subscribe(topic)
      _ <- consume(State.Empty)
    } yield {}

    // TODO rename
    val result2 = result.catchAll { failure =>
      failure.printStackTrace() // TODO
      log.error(s"failed: $failure", failure)
    }

    new TopicReplicator[F] {

      def shutdown() = {
        for {
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
}


