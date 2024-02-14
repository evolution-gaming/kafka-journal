package com.evolutiongaming.kafka.journal

import com.evolutiongaming.kafka.journal.eventual.EventualPayloadAndType

/** Snapshot storage for Kafka Journal.
  *
  * Uses `Key` instead of `persistenceId`, similar to [[ReplicatedJournalFlat]]
  * and [[EventualJournal]].
  */
trait SnapshotStoreFlat[F[_]] {

  /** Save snapshot for a specific key.
    *
    * @param key
    *   Unique identifier of a journal snapshot is done for.
    * @param snapshot
    *   Journal snapshot including sequence number and some metadata.
    */
  def save(key: Key, snapshot: SnapshotRecord[EventualPayloadAndType]): F[Unit]

  /** Loads a snapshot for a given key and selection criteria.
    *
    * If several snapshots are found using a passed selection criteria for a
    * specific key, then the last one (i.e. with a latest `SeqNr`) is returned.
    *
    * @param key
    *   Unique identifier of a journal snapshot was done for.
    * @param criteria
    *   Criteria to use.
    */
  def load(key: Key, criteria: SnapshotSelectionCriteria): F[Option[SnapshotRecord[EventualPayloadAndType]]]

  /** Deletes all snapshots for a given key and selection criteria.
    *
    * If several snapshots are found using a passed selection criteria for a
    * specific key, then all of them are deleted.
    *
    * @param key
    *   Unique identifier of a journal snapshot was done for.
    * @param criteria
    *   Criteria to use.
    */
  def delete(key: Key, criteria: SnapshotSelectionCriteria): F[Unit]

  /** Deletes a snapshot for a given key and sequence number.
    *
    * The method returns the same as
    * `load(key, SnapshotSelectionCriteria.one(seqNr))`,
    * but additional optimizations might be possible.
    *
    * @param key
    *   Unique identifier of a journal snapshot was done for.
    * @param seqNr
    *   Sequence number to be deleted.
    */
  def delete(key: Key, seqNr: SeqNr): F[Unit]

}
