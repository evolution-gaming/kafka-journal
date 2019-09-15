package com.evolutiongaming.kafka.journal.eventual.cassandra

import com.evolutiongaming.scassandra.{DecodeByName, DecodeRow, EncodeByName, EncodeRow}
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


final case class SegmentNr(value: Long) extends Ordered[SegmentNr] {

  require(value >= 0, s"invalid SegmentNr $value, it must be greater or equal 0")

  def compare(that: SegmentNr): Int = this.value compare that.value

  // TODO test this
  def to(segment: SegmentNr): List[SegmentNr] = {
    if (this == segment) List(segment)
    else {
      val range = (this.value to segment.value).toList
      range.map { value => SegmentNr(value) }
    }
  }

  override def toString = value.toString
}

object SegmentNr {

  implicit val encodeByNameSegmentNr: EncodeByName[SegmentNr] = EncodeByName[Long].imap(_.value)

  implicit val decodeByNameSegmentNr: DecodeByName[SegmentNr] = DecodeByName[Long].map(SegmentNr(_))


  implicit val encodeRowSegmentNr: EncodeRow[SegmentNr] = EncodeRow[SegmentNr]("segment")

  implicit val decodeRowSegmentNr: DecodeRow[SegmentNr] = DecodeRow[SegmentNr]("segment")
  

  def apply(seqNr: SeqNr, size: Int): SegmentNr = {
    val value = (seqNr.value - 1) / size
    SegmentNr(value)
  }
}