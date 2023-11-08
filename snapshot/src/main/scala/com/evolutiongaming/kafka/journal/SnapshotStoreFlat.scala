package com.evolutiongaming.kafka.journal

import com.evolutiongaming.kafka.journal.eventual.EventualPayloadAndType

import java.time.Instant

trait SnapshotStoreFlat[F[_]] {

  def save(key: Key, snapshot: SnapshotRecord[EventualPayloadAndType]): F[Unit]

  def load(
    key: Key,
    maxSeqNr: SeqNr,
    maxTimestamp: Instant,
    minSeqNr: SeqNr,
    minTimestamp: Instant
  ): F[Option[SnapshotRecord[EventualPayloadAndType]]]

  def drop(key: Key, maxSeqNr: SeqNr, maxTimestamp: Instant, minSeqNr: SeqNr, minTimestamp: Instant): F[Unit]

  def drop(key: Key, seqNr: SeqNr): F[Unit]

}
