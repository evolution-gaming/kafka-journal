package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.syntax.all.*
import com.evolutiongaming.kafka.journal.SeqNr

final case class Segment(nr: SegmentNr, size: SegmentSize)

object Segment {

  def apply(seqNr: SeqNr, size: SegmentSize): Segment = {
    val segmentNr = SegmentNr(seqNr, size)
    apply(segmentNr, size)
  }

  implicit class SegmentOps(val self: Segment) extends AnyVal {

    def next(seqNr: SeqNr): Option[Segment] = {
      val segmentNr = SegmentNr(seqNr, self.size)
      if (segmentNr === self.nr) none
      else self.copy(segmentNr).some
    }
  }
}
