package com.evolution.kafka.journal.eventual.cassandra

import cats.syntax.all.*
import com.evolution.kafka.journal.SeqNr

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
