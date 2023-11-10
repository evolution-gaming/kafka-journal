package com.evolutiongaming.kafka.journal

import com.evolutiongaming.kafka.journal.eventual.EventualPayloadAndType

trait SnapshotStoreFlat[F[_]] {

  def save(key: Key, snapshot: SnapshotRecord[EventualPayloadAndType]): F[Unit]

  def load(key: Key, criteria: SnapshotSelectionCriteria): F[Option[SnapshotRecord[EventualPayloadAndType]]]

  def drop(key: Key, criteria: SnapshotSelectionCriteria): F[Unit]

  def drop(key: Key, seqNr: SeqNr): F[Unit]

}
