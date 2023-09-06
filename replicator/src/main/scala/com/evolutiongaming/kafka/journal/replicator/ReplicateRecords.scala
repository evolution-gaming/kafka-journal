package com.evolutiongaming.kafka.journal.replicator

import cats.data.{NonEmptyList => Nel}
import cats.effect._
import cats.syntax.all._
import com.evolutiongaming.catshelper.ClockHelper._
import com.evolutiongaming.catshelper.{BracketThrowable, Log}
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.kafka.journal.conversions.{ConsRecordToActionRecord, KafkaRead}
import com.evolutiongaming.kafka.journal.eventual._
import com.evolutiongaming.kafka.journal.util.TemporalHelper._
import com.evolutiongaming.skafka.Offset

import java.time.Instant
import scala.concurrent.duration.FiniteDuration


trait ReplicateRecords[F[_]] {

  def apply(records: Nel[ConsRecord], timestamp: Instant): F[Int]
}

object ReplicateRecords {

  def apply[F[_]: BracketThrowable: Clock, A](
    consRecordToActionRecord: ConsRecordToActionRecord[F],
    journal: ReplicatedKeyJournal[F],
    metrics: TopicReplicatorMetrics[F],
    kafkaRead: KafkaRead[F, A],
    eventualWrite: EventualWrite[F, A],
    log: Log[F]
  ): ReplicateRecords[F] = {

    (records: Nel[ConsRecord], timestamp: Instant) => {

      def apply(records: Nel[ActionRecord[Action]]) = {
        val record = records.last
        val key = record.action.key
        val partition = record.partitionOffset.partition
        val id = key.id

        def measurements(records: Int) = {
          for {
            now <- Clock[F].instant
          } yield {
            val timestamp1 = record.action.timestamp
            TopicReplicatorMetrics.Measurements(
              replicationLatency = now diff timestamp1,
              deliveryLatency = timestamp diff timestamp1,
              records = records)
          }
        }

        def append(offset: Offset, records: Nel[ActionRecord[Action.Append]]) = {
          val bytes = records.foldLeft(0L) { case (bytes, record) => bytes + record.action.payload.size }

          def msg(
            events: Nel[EventRecord[EventualPayloadAndType]],
            latency: FiniteDuration,
            expireAfter: Option[ExpireAfter]
          ) = {
            val seqNrs =
              if (events.tail.isEmpty) s"seqNr: ${ events.head.seqNr }"
              else s"seqNrs: ${ events.head.seqNr }..${ events.last.seqNr }"
            val origin = records.head.action.origin
            val originStr = origin.foldMap { origin => s", origin: $origin" }
            val version = records.last.action.version
            val versionStr = version.fold("none") { _.toString }
            val expireAfterStr = expireAfter.foldMap { expireAfter => s", expireAfter: $expireAfter" }
            s"append in ${ latency.toMillis }ms, id: $id, partition: $partition, offset: $offset, $seqNrs$originStr, version: $versionStr$expireAfterStr"
          }

          def measure(events: Nel[EventRecord[EventualPayloadAndType]], expireAfter: Option[ExpireAfter]) = {
            for {
              measurements <- measurements(records.size)
              result       <- metrics.append(events = events.length, bytes = bytes, measurements = measurements)
              _            <- log.info(msg(events, measurements.replicationLatency, expireAfter))
            } yield result
          }

          for {
            events      <- records.flatTraverse { record =>
              val action = record.action
              val payloadAndType = action.toPayloadAndType
              for {
                events         <- kafkaRead(payloadAndType).adaptError { case e =>
                  JournalError(s"ReplicateRecords failed for id: $id, partition: $partition, offset: $offset: $e", e)
                }
                eventualEvents <- events.events.traverse { _.traverse { a => eventualWrite(a) } }
              } yield for {
                event <- eventualEvents
              } yield {
                EventRecord(record, event, events.metadata)
              }
            }
            expireAfter  = events.last.metadata.payload.expireAfter
            result      <- journal.append(offset, timestamp, expireAfter, events)
            result      <- if (result) measure(events, expireAfter).as(events.size) else 0.pure[F]
          } yield result
        }

        def delete(offset: Offset, deleteTo: DeleteTo, origin: Option[Origin], version: Option[Version]) = {

          def msg(latency: FiniteDuration) = {
            val originStr = origin.foldMap { origin => s", origin: $origin" }
            val versionStr = version.fold("none") { _.toString }
            s"delete in ${ latency.toMillis }ms, id: $id, offset: $partition:$offset, deleteTo: $deleteTo$originStr, version: $versionStr"
          }

          def measure() = {
            for {
              measurements <- measurements(1)
              latency       = measurements.replicationLatency
              _            <- metrics.delete(measurements)
              result       <- log.info(msg(latency))
            } yield result
          }

          for {
            result <- journal.delete(offset, timestamp, deleteTo, origin)
            result <- if (result) measure().as(1) else 0.pure[F]
          } yield result
        }

        def purge(offset: Offset, origin: Option[Origin], version: Option[Version]) = {

          def msg(latency: FiniteDuration) = {
            val originStr = origin.foldMap { origin => s", origin: $origin" }
            val versionStr = version.fold("none") { _.toString }
            s"purge in ${ latency.toMillis }ms, id: $id, offset: $partition:$offset$originStr, version: $versionStr"
          }

          def measure() = {
            for {
              measurements <- measurements(1)
              latency       = measurements.replicationLatency
              _            <- metrics.purge(measurements)
              result       <- log.info(msg(latency))
            } yield result
          }

          for {
            result <- journal.purge(offset, timestamp)
            result <- if (result) measure().as(1) else 0.pure[F]
          } yield result
        }

        Batch
          .of(records)
          .foldMapM {
            case batch: Batch.Appends => append(batch.offset, batch.records)
            case batch: Batch.Delete  => delete(batch.offset, batch.to, batch.origin, batch.version)
            case batch: Batch.Purge   => purge(batch.offset, batch.origin, batch.version)
          }
      }

      for {
        records <- records
          .toList
          .traverseFilter { a => consRecordToActionRecord(a).value }
        result  <- records
          .toNel
          .foldMapM { records => apply(records) }
      } yield result
    }
  }
}
