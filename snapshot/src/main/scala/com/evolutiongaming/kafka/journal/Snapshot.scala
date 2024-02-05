package com.evolutiongaming.kafka.journal

/** Represents a snapshot to be stored or loaded from a storage.
  *
  * @param seqNr
  *   Snapshot sequence number.
  * @param payload
  *   Actual contents of a snapshot. The value is never `None` under normal circumstances.
  */
final case class Snapshot[A](
  seqNr: SeqNr,
  payload: Option[A]
)
