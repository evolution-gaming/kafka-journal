package com.evolutiongaming.kafka.journal

import com.evolutiongaming.kafka.journal.SeqNr.Helper._
import com.evolutiongaming.nel.Nel

// TODO test
// TODO add method with single argument of range size 1

final case class SeqRange(from: SeqNr, to: SeqNr) {

  require(from <= to, s"from($from) <= to($to)")

  def <(seqNr: SeqNr): Boolean = to < seqNr

  def >(seqNr: SeqNr): Boolean = from > seqNr

  def <(range: SeqRange): Boolean = this < range.from

  def >(range: SeqRange): Boolean = this > range.to

  //  def <=(seqNr: SeqNr): Boolean = to <= seqNr
  //
  //  def >=(seqNr: SeqNr): Boolean = from >= seqNr
  //
  //  def <=(range: SeqRange): Boolean = this <= range.from
  //
  //  def >=(range: SeqRange): Boolean = this >= range.to
  //
  def intersects(range: SeqRange): Boolean = {
    !(this > range || this < range)
  }

  def contains(seqNr: SeqNr): Boolean = from <= seqNr && to >= seqNr

  def contains(range: SeqRange): Boolean = from <= range.from && to >= range.to

  // TODO implement properly
  // TODO rename
  def seqNrs: Nel[SeqNr] = Nel.unsafe((from.value to to.value).toList).map(SeqNr(_))

  override def toString: String = {
    if (from == to) from.toString
    else s"$from..$to"
  }
}

object SeqRange {

  val All: SeqRange = SeqRange(SeqNr.Min, SeqNr.Max)

  def apply(value: SeqNr): SeqRange = SeqRange(value, value)

  def apply(value: Long): SeqRange = SeqRange(value.toSeqNr)

  def apply(from: Long, to: Long): SeqRange = SeqRange(from = from.toSeqNr, to = to.toSeqNr)

  // TODO move to SeqNr
  implicit class SeqNrOps(val self: SeqNr) extends AnyVal {
    def <(range: SeqRange): Boolean = range > self
    def >(range: SeqRange): Boolean = range < self
  }
}