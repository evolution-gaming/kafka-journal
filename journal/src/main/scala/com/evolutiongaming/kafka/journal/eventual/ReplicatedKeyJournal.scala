package com.evolutiongaming.kafka.journal.eventual

import java.time.Instant

import cats.data.{NonEmptyList => Nel}
import com.evolutiongaming.kafka.journal._

import scala.concurrent.duration.FiniteDuration

trait ReplicatedKeyJournal[F[_]] {

  def append(
    partitionOffset: PartitionOffset,
    timestamp: Instant,
    expireAfter: Option[FiniteDuration],
    events: Nel[EventRecord]
  ): F[Unit]

  def delete(
    partitionOffset: PartitionOffset,
    timestamp: Instant,
    deleteTo: SeqNr,
    origin: Option[Origin]
  ): F[Unit]
}

object ReplicatedKeyJournal {

  def apply[F[_]](key: Key, replicatedJournal: ReplicatedJournalOld[F]): ReplicatedKeyJournal[F] = {

    new ReplicatedKeyJournal[F] {

      def append(
        partitionOffset: PartitionOffset,
        timestamp: Instant,
        expireAfter: Option[FiniteDuration],
        events: Nel[EventRecord]
      ) = {
        replicatedJournal.append(key, partitionOffset, timestamp, expireAfter, events)
      }

      def delete(
        partitionOffset: PartitionOffset,
        timestamp: Instant,
        deleteTo: SeqNr,
        origin: Option[Origin]
      ) = {
        replicatedJournal.delete(key, partitionOffset, timestamp, deleteTo, origin)
      }
    }
  }
}