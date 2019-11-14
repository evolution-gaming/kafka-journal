package com.evolutiongaming.kafka.journal.replicator

import java.time.Instant

import cats.data.{NonEmptyList => Nel, NonEmptyMap => Nem}
import cats.effect._
import cats.implicits._
import cats.{Monad, Parallel}
import com.evolutiongaming.catshelper.ClockHelper._
import com.evolutiongaming.catshelper.Log
import com.evolutiongaming.catshelper.ParallelHelper._
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.kafka.journal.conversions.{ConsumerRecordToActionRecord, PayloadToEvents}
import com.evolutiongaming.kafka.journal.eventual._
import com.evolutiongaming.kafka.journal.replicator.TopicReplicator.{Metrics, State}
import com.evolutiongaming.kafka.journal.util.TemporalHelper._
import com.evolutiongaming.skafka.{Bytes => _, _}

import scala.concurrent.duration.FiniteDuration


trait ReplicateRecords[F[_]] {

  def apply(
    state: State,
    consumerRecords: Nem[TopicPartition, Nel[ConsRecord]],
    roundStart: Instant
  ): F[State]
}

object ReplicateRecords {

  def apply[F[_] : Monad : Clock : Parallel](
    topic: Topic,
    consumerRecordToActionRecord: ConsumerRecordToActionRecord[F],
    journal: ReplicatedJournal[F],
    metrics: Metrics[F],
    payloadToEvents: PayloadToEvents[F],
    log: Log[F]
  ): ReplicateRecords[F] = {

    new ReplicateRecords[F] {

      def apply(
        state: State,
        consumerRecords: Nem[TopicPartition, Nel[ConsRecord]],
        roundStart: Instant
      ) = {

        def apply(records: Nel[ActionRecord[Action]]) = {
          val head = records.head
          val key = head.action.key
          val id = key.id

          def measurements(records: Int) = {
            for {
              now <- Clock[F].instant
            } yield {
              Metrics.Measurements(
                partition = head.partition,
                replicationLatency = now diff head.action.timestamp,
                deliveryLatency = roundStart diff head.action.timestamp,
                records = records)
            }
          }

          def delete(partitionOffset: PartitionOffset, deleteTo: SeqNr, origin: Option[Origin]) = {

            def msg(latency: FiniteDuration) = {
              val originStr = origin.fold("") { origin => s", origin: $origin" }
              s"delete in ${ latency.toMillis }ms, id: $id, offset: $partitionOffset, deleteTo: $deleteTo$originStr"
            }

            for {
              _            <- journal.delete(key, partitionOffset, roundStart, deleteTo, origin)
              measurements <- measurements(1)
              latency       = measurements.replicationLatency
              _            <- metrics.delete(measurements)
              _            <- log.info(msg(latency))
            } yield {}
          }


          def append(partitionOffset: PartitionOffset, records: Nel[ActionRecord[Action.Append]]) = {

            val bytes = records.foldLeft(0L) { case (bytes, record) => bytes + record.action.payload.size }

            val events = records.flatTraverse { record =>
              val action = record.action
              val payloadAndType = PayloadAndType(action)
              for {
                events <- payloadToEvents(payloadAndType)
              } yield for {
                event <- events.events
              } yield {
                EventRecord(record, event)
              }
            }

            val expireAfter = records.last.action.header.expireAfter

            def msg(events: Nel[EventRecord], latency: FiniteDuration) = {
              val seqNrs =
                if (events.tail.isEmpty) s"seqNr: ${ events.head.seqNr }"
                else s"seqNrs: ${ events.head.seqNr }..${ events.last.seqNr }"
              val origin = records.head.action.origin
              val originStr = origin.fold("") { origin => s", origin: $origin" }
              val expireAfterStr = expireAfter.fold("") { expireAfter => s", expireAfter: $expireAfter" }
              s"append in ${ latency.toMillis }ms, id: $id, offset: $partitionOffset, $seqNrs$originStr$expireAfterStr"
            }

            for {
              events       <- events
              _            <- journal.append(key, partitionOffset, roundStart, expireAfter, events)
              measurements <- measurements(records.size)
              _            <- metrics.append(events = events.length, bytes = bytes, measurements = measurements)
              _            <- log.info(msg(events, measurements.replicationLatency))
            } yield {}
          }

          Batch
            .of(records)
            .foldMapM {
              case batch: Batch.Appends => append(batch.partitionOffset, batch.records)
              case batch: Batch.Delete  => delete(batch.partitionOffset, batch.seqNr, batch.origin)
            }
        }

        val pointers = consumerRecords
          .toNel
          .map { case (topicPartition, records) =>
            val offset = records.foldLeft(Offset.Min) { (offset, record) => record.offset max offset }
            (topicPartition.partition, offset)
          }
          .toNem

        val replicate = consumerRecords
          .toSortedMap
          .values
          .toList
          .parFoldMap { records =>
            records
              .groupBy { _.key.map { _.value } }
              .values
              .toList
              .parFoldMap { records =>
                for {
                  records <- records.toList.traverseFilter { record => consumerRecordToActionRecord(record) }
                  result  <- records.toNel.traverse { records => apply(records) }
                } yield result
              }
        }

        for {
          _ <- replicate
          _ <- journal.save(topic, pointers, roundStart)
        } yield {
          state.copy(pointers = state.pointers + TopicPointers(pointers.toSortedMap)/*TODO not use it*/)
        }
      }
    }
  }
}
