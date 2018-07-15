package com.evolutiongaming.kafka.journal.eventual.cassandra

import com.evolutiongaming.kafka.journal.Alias.SeqNr

import scala.collection.immutable.Seq


case class Segment(nr: SegmentNr, size: Int) {

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


case class SegmentNr(value: Long) extends Ordered[SegmentNr] {

  def compare(that: SegmentNr): Int = this.value compare that.value

  def to(segment: SegmentNr): Seq[SegmentNr] = {
    if (this == segment) Seq.empty
    else {
      val range = this.value to segment.value
      range.map { value => SegmentNr(value) }
    }
  }
}

object SegmentNr {

  def apply(seqNr: SeqNr, size: Int): SegmentNr = {
    val value = ((seqNr - 1) max 0) / size
    SegmentNr(value)
  }
}