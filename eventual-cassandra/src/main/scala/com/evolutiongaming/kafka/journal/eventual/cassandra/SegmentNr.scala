package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.kernel.Eq
import cats.syntax.all._
import cats.{Applicative, Id, Monad, Order, Show}
import com.evolutiongaming.kafka.journal.SeqNr
import com.evolutiongaming.kafka.journal.util.Fail
import com.evolutiongaming.kafka.journal.util.Fail.implicits._
import com.evolutiongaming.scassandra.{DecodeByName, DecodeRow, EncodeByName, EncodeRow}

sealed abstract case class SegmentNr(value: Long) {

  override def toString: String = value.toString
}

object SegmentNr {

  val min: SegmentNr = new SegmentNr(0L) {}

  val max: SegmentNr = new SegmentNr(Long.MaxValue) {}

  implicit val eqSegmentNr: Eq[SegmentNr] = Eq.fromUniversalEquals

  implicit val showSegmentNr: Show[SegmentNr] = Show.fromToString

  implicit val orderingSegmentNr: Ordering[SegmentNr] = Ordering.by(_.value)

  implicit val orderSegmentNr: Order[SegmentNr] = Order.fromOrdering

  implicit val encodeByNameSegmentNr: EncodeByName[SegmentNr] = EncodeByName[Long].contramap(_.value)

  implicit val decodeByNameSegmentNr: DecodeByName[SegmentNr] = DecodeByName[Long].map(a => SegmentNr.of[Id](a))

  implicit val encodeRowSegmentNr: EncodeRow[SegmentNr] = EncodeRow[SegmentNr]("segment")

  implicit val decodeRowSegmentNr: DecodeRow[SegmentNr] = DecodeRow[SegmentNr]("segment")

  def of[F[_]: Applicative: Fail](value: Long): F[SegmentNr] =
    if (value < min.value) {
      s"invalid SegmentNr of $value, it must be greater or equal to $min".fail[F, SegmentNr]
    } else if (value > max.value) {
      s"invalid SegmentNr of $value, it must be less or equal to $max".fail[F, SegmentNr]
    } else if (value === min.value) {
      min.pure[F]
    } else if (value === max.value) {
      max.pure[F]
    } else {
      new SegmentNr(value) {}.pure[F]
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

  def unsafe[A](value: A)(implicit numeric: Numeric[A]): SegmentNr = of[Id](numeric.toLong(value))

  def fromSegments(segments: Segments): List[SegmentNr] =
    min.value
      .until(segments.value.toLong)
      .map(a => new SegmentNr(a) {})
      .toList

  implicit class SegmentNrOps(val self: SegmentNr) extends AnyVal {

    def to[F[_]: Monad: Fail](segmentNr: SegmentNr): F[List[SegmentNr]] =
      if (self === segmentNr) List(segmentNr).pure[F]
      else if (self > segmentNr) List.empty[SegmentNr].pure[F]
      else {
        (segmentNr, List.empty[SegmentNr]).tailRecM {
          case (segmentNr, segmentNrs) =>
            if (segmentNr === self) {
              (segmentNr :: segmentNrs)
                .asRight[(SegmentNr, List[SegmentNr])]
                .pure[F]
            } else {
              segmentNr
                .prev[F]
                .map(prev => (prev, segmentNr :: segmentNrs).asLeft[List[SegmentNr]])
            }
        }
      }

    def map[F[_]: Applicative: Fail](f: Long => Long): F[SegmentNr] = SegmentNr.of[F](f(self.value))

    def next[F[_]: Applicative: Fail]: F[SegmentNr] = map(_ + 1L)

    def prev[F[_]: Applicative: Fail]: F[SegmentNr] = map(_ - 1L)
  }

  object implicits {
    implicit class SeqNrOpsSegmentNr(val self: SeqNr) extends AnyVal {
      def toSegmentNr(segmentSize: SegmentSize): SegmentNr = SegmentNr(self, segmentSize)
    }
  }
}
