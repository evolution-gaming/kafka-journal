package com.evolutiongaming.kafka.journal

import com.evolutiongaming.kafka.journal.Alias._

// TODO test
// TODO add method with single argument of range size 1

case class SeqRange(from: SeqNr, to: SeqNr) {

  require(from <= to, s"from($from) <= to($to)")
  require(from >= 0, s"from($from) >= 0")
  require(to >= 0, s"to($to) >= 0")

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

  override def toString: String = {
    if (from == to) from.toString
    else s"$from..$to"
  }
}

object SeqRange {

  val All: SeqRange = SeqRange(SeqNr.Min, SeqNr.Max)

  def apply(value: SeqNr): SeqRange = SeqRange(value, value)

  implicit class SeqNrOps(val self: SeqNr) extends AnyVal {
    def <(range: SeqRange): Boolean = range > self
    def >(range: SeqRange): Boolean = range < self
  }
}