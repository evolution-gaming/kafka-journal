package com.evolutiongaming.kafka.journal.eventual.cassandra

import com.evolutiongaming.kafka.journal.SeqNr


final case class Segment(nr: SegmentNr, size: Int) {

  require(size > 1, s"invalid size $size, it must be greater than 1")

  def next(seqNr: SeqNr): Option[Segment] = {
    val segmentNr = SegmentNr(seqNr, size)
    if (segmentNr == nr) None
    else Some(copy(nr = segmentNr))
  }
}

object Segment {
  def apply(seqNr: SeqNr, size: Int): Segment = {
    val segment = SegmentNr(seqNr, size)
    Segment(segment, size)
  }
}