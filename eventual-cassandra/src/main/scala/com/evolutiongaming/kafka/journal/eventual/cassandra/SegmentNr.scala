package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.data.{NonEmptyList => Nel}
import cats.implicits._
import cats.kernel.Eq
import cats.{Order, Show}
import com.evolutiongaming.kafka.journal.SeqNr
import com.evolutiongaming.kafka.journal.util.ApplicativeString
import com.evolutiongaming.kafka.journal.util.OptionHelper._
import com.evolutiongaming.kafka.journal.util.TryHelper._
import com.evolutiongaming.scassandra.{DecodeByName, DecodeRow, EncodeByName, EncodeRow}

import scala.util.Try

sealed abstract case class SegmentNr(value: Long) {

  override def toString: String = value.toString
}

object SegmentNr {

  val min: SegmentNr = new SegmentNr(0L) {}

  val max: SegmentNr = new SegmentNr(Long.MaxValue) {}


  implicit val eqSegmentNr: Eq[SegmentNr] = Eq.fromUniversalEquals

  implicit val showSeqNr: Show[SegmentNr] = Show.fromToString


  implicit val encodeByNameSegmentNr: EncodeByName[SegmentNr] = EncodeByName[Long].contramap(_.value)

  implicit val decodeByNameSegmentNr: DecodeByName[SegmentNr] = DecodeByName[Long].map { a => SegmentNr.of[Try](a).get }


  implicit val encodeRowSegmentNr: EncodeRow[SegmentNr] = EncodeRow[SegmentNr]("segment")

  implicit val decodeRowSegmentNr: DecodeRow[SegmentNr] = DecodeRow[SegmentNr]("segment")


  implicit val orderingSegmentNr: Ordering[SegmentNr] = Ordering.by(_.value)

  implicit val orderSegmentNr: Order[SegmentNr] = Order.fromOrdering


  def of[F[_] : ApplicativeString](value: Long): F[SegmentNr] = {
    if (value < min.value) {
      s"invalid SegmentNr of $value, it must be greater or equal to $min".raiseError[F, SegmentNr]
    } else if (value > max.value) {
      s"invalid SegmentNr of $value, it must be less or equal to $max".raiseError[F, SegmentNr]
    } else if (value === min.value) {
      min.pure[F]
    } else if (value === max.value) {
      max.pure[F]
    } else {
      new SegmentNr(value) {}.pure[F]
    }
  }


  def apply(seqNr: SeqNr, segmentSize: SegmentSize): SegmentNr = {
    val segmentNr = (seqNr.value - SeqNr.min.value) / segmentSize.value
    new SegmentNr(segmentNr) {}
  }


  def apply(hashCode: Int, segments: Segments): SegmentNr = {
    val segmentNr = math.abs(hashCode.toLong % segments.value)
    new SegmentNr(segmentNr) {}
  }


  def opt(value: Long): Option[SegmentNr] = of[Option](value)


  def unsafe[A](value: A)(implicit numeric: Numeric[A]): SegmentNr = of[Try](numeric.toLong(value)).get


  implicit class SegmentNrOps(val self: SegmentNr) extends AnyVal {
    
    // TODO test this
    // TODO stop using this unsafe
    def to(segment: SegmentNr): Nel[SegmentNr] = {
      if (self === segment) Nel.of(segment)
      else {
        val range = Nel.fromListUnsafe((self.value to segment.value).toList) // TODO remove fromListUnsafe
        range.map { value => SegmentNr.unsafe(value) } // TODO stop using unsafe
      }
    }
  }
}