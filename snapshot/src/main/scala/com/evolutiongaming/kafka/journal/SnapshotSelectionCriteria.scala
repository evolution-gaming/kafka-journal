package com.evolutiongaming.kafka.journal

import java.time.Instant

final case class SnapshotSelectionCriteria(
  maxSeqNr: SeqNr = SeqNr.max,
  maxTimestamp: Instant = Instant.MAX,
  minSeqNr: SeqNr = SeqNr.min,
  minTimestamp: Instant = Instant.MIN
)

object SnapshotSelectionCriteria {

  val All: SnapshotSelectionCriteria = SnapshotSelectionCriteria()

  def one(seqNr: SeqNr): SnapshotSelectionCriteria =
    SnapshotSelectionCriteria(maxSeqNr = seqNr, minSeqNr = seqNr)

}
