package com.evolutiongaming.kafka.journal

import cats.data.{NonEmptyList => Nel}
import com.evolutiongaming.kafka.journal.SeqNr.implicits._
import play.api.libs.json.{Json, OFormat}

import scala.annotation.tailrec

final case class SeqRange(from: SeqNr, to: SeqNr) {

  require(from <= to, s"from($from) <= to($to)")

  def <(seqNr: SeqNr): Boolean = to < seqNr

  def >(seqNr: SeqNr): Boolean = from > seqNr

  def <(range: SeqRange): Boolean = this < range.from

  def >(range: SeqRange): Boolean = this > range.to

  def intersects(range: SeqRange): Boolean = {
    !(this > range || this < range)
  }

  def contains(seqNr: SeqNr): Boolean = from <= seqNr && to >= seqNr

  def contains(range: SeqRange): Boolean = from <= range.from && to >= range.to

  def toNel: Nel[SeqNr] = {

    @tailrec
    def loop(xs: Nel[SeqNr]): Nel[SeqNr] = xs.head.prev match {
      case Some(seqNr) if seqNr >= from => loop(seqNr :: xs)
      case _                            => xs
    }

    loop(Nel.of(to))
  }

  override def toString: String = {
    if (from == to) from.toString
    else s"$from..$to"
  }
}

object SeqRange {

  implicit val Format: OFormat[SeqRange] = Json.format

  val All: SeqRange = SeqRange(SeqNr.Min, SeqNr.Max)

  def apply(value: SeqNr): SeqRange = SeqRange(value, value)

  def apply(value: Long): SeqRange = SeqRange(value.toSeqNr)

  def apply(from: Long, to: Long): SeqRange = SeqRange(from = from.toSeqNr, to = to.toSeqNr)
}