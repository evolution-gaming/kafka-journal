package com.evolutiongaming.kafka.journal.eventual.cassandra

import com.evolutiongaming.kafka.journal.SeqNr
import com.evolutiongaming.scassandra.{DecodeByName, DecodeRow, EncodeByName, EncodeRow}

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