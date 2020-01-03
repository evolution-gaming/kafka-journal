package com.evolutiongaming.kafka.journal.replicator

import java.time.Instant

import cats.data.{NonEmptyList => Nel}
import cats.effect._
import cats.implicits._
import cats.Parallel
import com.evolutiongaming.catshelper.ClockHelper._
import com.evolutiongaming.catshelper.{BracketThrowable, Log}
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.kafka.journal.conversions.{ConsRecordToActionRecord, PayloadToEvents}
import com.evolutiongaming.kafka.journal.eventual._
import com.evolutiongaming.kafka.journal.replicator.TopicReplicator.Metrics
import com.evolutiongaming.kafka.journal.util.TemporalHelper._

import scala.concurrent.duration.FiniteDuration


trait ReplicateRecords[F[_]] {

  def apply(records: Nel[ConsRecord], timestamp: Instant): F[Unit]
}

object ReplicateRecords {

  def apply[F[_] : BracketThrowable : Clock : Parallel](
    consRecordToActionRecord: ConsRecordToActionRecord[F],
    journal: ReplicatedKeyJournal[F],
    metrics: Metrics[F],
    payloadToEvents: PayloadToEvents[F],
    log: Log[F]
  ): ReplicateRecords[F] = {

    new ReplicateRecords[F] {

      def apply(records: Nel[ConsRecord], timestamp: Instant) = {

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
                deliveryLatency = timestamp diff head.action.timestamp,
                records = records)
            }
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
                EventRecord(record, event, events.metadata)
              }
            }

            def msg(events: Nel[EventRecord], latency: FiniteDuration, expireAfter: Option[ExpireAfter]) = {
              val seqNrs =
                if (events.tail.isEmpty) s"seqNr: ${ events.head.seqNr }"
                else s"seqNrs: ${ events.head.seqNr }..${ events.last.seqNr }"
              val origin = records.head.action.origin
              val originStr = origin.foldMap { origin => s", origin: $origin" }
              val expireAfterStr = expireAfter.foldMap { expireAfter => s", expireAfter: $expireAfter" }
              s"append in ${ latency.toMillis }ms, id: $id, offset: $partitionOffset, $seqNrs$originStr$expireAfterStr"
            }

            for {
              events       <- events
              expireAfter   = events.last.metadata.payload.expireAfter
              _            <- journal.append(partitionOffset, timestamp, expireAfter, events)
              measurements <- measurements(records.size)
              _            <- metrics.append(events = events.length, bytes = bytes, measurements = measurements)
              _            <- log.info(msg(events, measurements.replicationLatency, expireAfter))
            } yield {}
          }

          def delete(partitionOffset: PartitionOffset, deleteTo: DeleteTo, origin: Option[Origin]) = {

            def msg(latency: FiniteDuration) = {
              val originStr = origin.foldMap { origin => s", origin: $origin" }
              s"delete in ${ latency.toMillis }ms, id: $id, offset: $partitionOffset, deleteTo: $deleteTo$originStr"
            }

            for {
              _            <- journal.delete(partitionOffset, timestamp, deleteTo, origin)
              measurements <- measurements(1)
              latency       = measurements.replicationLatency
              _            <- metrics.delete(measurements)
              _            <- log.info(msg(latency))
            } yield {}
          }

          def purge(partitionOffset: PartitionOffset, origin: Option[Origin]) = {

            def msg(latency: FiniteDuration) = {
              val originStr = origin.foldMap { origin => s", origin: $origin" }
              s"purge in ${ latency.toMillis }ms, id: $id, offset: $partitionOffset$originStr"
            }

            for {
              _            <- journal.purge(partitionOffset.offset, timestamp)
              measurements <- measurements(1)
              latency       = measurements.replicationLatency
              _            <- metrics.purge(measurements)
              _            <- log.info(msg(latency))
            } yield {}
          }

          Batch
            .of(records)
            .foldMapM {
              case batch: Batch.Appends => append(batch.partitionOffset, batch.records)
              case batch: Batch.Delete  => delete(batch.partitionOffset, batch.to, batch.origin)
              case batch: Batch.Purge   => purge(batch.partitionOffset, batch.origin)
            }
        }

        for {
          records <- records.toList.traverseFilter { a => consRecordToActionRecord(a).value }
          _       <- records.toNel.traverse { records => apply(records) }
        } yield {}
      }
    }
  }
}
