package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.implicits._
import com.evolutiongaming.kafka.journal.SeqNr


sealed abstract case class Segment(nr: SegmentNr, size: Int)

object Segment {

  // TODO stop using this
  def unsafe(seqNr: SeqNr, size: Int): Segment = {
    require(size > 1, s"invalid size $size, it must be greater than 1")
    val segmentNr = SegmentNr.unsafe(seqNr, size)
    unsafe(segmentNr, size)
  }

  // TODO stop using this
  def unsafe(segmentNr: SegmentNr, size: Int): Segment = {
    require(size > 1, s"invalid size $size, it must be greater than 1")
    new Segment(segmentNr, size) {}
  }


  implicit class SegmentOps(val self: Segment) extends AnyVal {
    // TODO stop using this
    def nextUnsafe(seqNr: SeqNr): Option[Segment] = {
      val segmentNr = SegmentNr.unsafe(seqNr, self.size)
      if (segmentNr == self.nr) none
      else unsafe(segmentNr, self.size).some
    }
  }
}