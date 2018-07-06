package com.evolutiongaming.kafka.journal

import com.evolutiongaming.kafka.journal.Alias.SeqNr

// TODO test
// TODO add method with single argument of range size 1
case class SeqRange(from: SeqNr = 0, to: SeqNr = SeqNr.Max) {
  require(from <= to, s"from must be <= to, but $from > $to") // TODO do we need error?
  require(from >= 0) // TODO
  require(to >= 0) // TODO


  def <(range: SeqRange): Boolean = this < range.from

  def >(range: SeqRange): Boolean = this > range.to

  def <(seqNr: SeqNr): Boolean = seqNr < from

  def >(seqNr: SeqNr): Boolean = seqNr > to

  def contains(seqNr: SeqNr): Boolean = from <= seqNr && to >= seqNr

  def contains(range: SeqRange): Boolean = from <= range.from && to >= range.to

  override def toString: String = {
    if (from == to) from.toString
    else s"$from..$to"
  }
}

object SeqRange {

  implicit class SeqNrOps(val self: SeqNr) extends AnyVal {
    def <(range: SeqRange): Boolean = range > self
    def >(range: SeqRange): Boolean = range < self
  }
}