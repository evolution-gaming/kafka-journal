package com.evolution.kafka.journal

/**
 * Represents a snapshot to be stored or loaded from a storage.
 *
 * @param seqNr
 *   Snapshot sequence number.
 * @param payload
 *   Actual contents of a snapshot.
 */
final case class Snapshot[A](
  seqNr: SeqNr,
  payload: A,
)
