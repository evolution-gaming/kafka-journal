package com.evolutiongaming.kafka.journal

import java.time.Instant

/** Type safer version of `akka.persistence.SnapshotSelectionCriteria`.
  * 
  * The important consideration when using this class is to always use named
  * parameters to consruct the instances of the class. Otherwise it is too
  * easy to confuse `maxSeqNr` and `minSeqNr`, especially, given the fact,
  * they are given in a reverse order in a constructor.
  */
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
