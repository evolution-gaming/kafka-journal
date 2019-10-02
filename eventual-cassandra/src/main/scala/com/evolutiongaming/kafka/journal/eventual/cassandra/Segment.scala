package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.implicits._
import com.evolutiongaming.kafka.journal.SeqNr


final case class Segment(nr: SegmentNr, size: SegmentSize)

object Segment {

  // TODO stop using this
  def unsafe(seqNr: SeqNr, size: SegmentSize): Segment = {
    val segmentNr = SegmentNr.unsafe(seqNr, size)
    apply(segmentNr, size)
  }


  implicit class SegmentOps(val self: Segment) extends AnyVal {

    // TODO stop using this
    def nextUnsafe(seqNr: SeqNr): Option[Segment] = {
      val segmentNr = SegmentNr.unsafe(seqNr, self.size)
      if (segmentNr == self.nr) none
      else self.copy(segmentNr).some
    }
  }
}