package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.syntax.all.*
import com.evolutiongaming.kafka.journal.SeqNr

/**
 * Represent a segment in the `journal` table.
 *
 * @param nr
 *   segment number
 * @param size
 *   maximum segment number
 */
private[journal] final case class Segment(nr: SegmentNr, size: SegmentSize)

private[journal] object Segment {

  @deprecated("use `Segment.journal` instead", "4.1.0")
  def apply(seqNr: SeqNr, size: SegmentSize): Segment = journal(seqNr, size)

  /**
   * Calculate segment number for `seqNr` based on `segmentSize` for `journal` table.
   *
   * @see
   *   based on [[SegmentNr#journal]]
   */
  def journal(seqNr: SeqNr, size: SegmentSize): Segment = {
    val segmentNr = SegmentNr.journal(seqNr, size)
    Segment(segmentNr, size)
  }

  implicit class SegmentOps(val self: Segment) extends AnyVal {

    /**
     * Get segment for `seqNr` if its different from current segment `self`.
     *
     * @param seqNr
     *   sequence number
     * @return
     *   [[Some]] segment if `seqNr` is in different segment, [[None]] otherwise
     */
    def next(seqNr: SeqNr): Option[Segment] = {
      val segmentNr = SegmentNr.journal(seqNr, self.size)
      if (segmentNr === self.nr) none
      else Segment(segmentNr, self.size).some
    }
  }
}
