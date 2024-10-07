package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.kernel.Eq
import cats.syntax.all.*
import cats.{Applicative, Id, Order, Show}
import com.evolutiongaming.kafka.journal.util.Fail
import com.evolutiongaming.kafka.journal.util.Fail.implicits.*

/** The maximum number of segments in 'metajournal' table.
  *
  * When [[Segments]] is used then the segment column value is determined by
  * consistent hashing of the key column. I.e. there always no more than
  * [[Segments#value]] different values. This allow selecting all records
  * in the table by iterating over all segments.
  *
  * The logic itself could be found in [[SegmentNr#metaJournal]].
  *
  * The value is not end-user configurable and, currently, set in
  * [[EventualCassandra]].
  *
  * @see [[SegmentNr]] for usage in `metajournal` table.
  * @see [[SegmentSize]] for alternative way, used in `journal` table.
  */
private[journal] sealed abstract case class Segments(value: Int) {

  override def toString: String = value.toString
}

private[journal] object Segments {

  val min: Segments = new Segments(1) {}

  val max: Segments = new Segments(Int.MaxValue) {}

  val old: Segments = new Segments(100) {}

  val default: Segments = new Segments(10000) {}

  implicit val eqSegments: Eq[Segments] = Eq.fromUniversalEquals

  implicit val showSegments: Show[Segments] = Show.fromToString

  implicit val orderingSegments: Ordering[Segments] = Ordering.by(_.value)

  implicit val orderSegments: Order[Segments] = Order.fromOrdering

  def of[F[_]: Applicative: Fail](value: Int): F[Segments] = {
    if (value < min.value) {
      s"invalid Segments of $value, it must be greater or equal to $min".fail[F, Segments]
    } else if (value > max.value) {
      s"invalid Segments of $value, it must be less or equal to $max".fail[F, Segments]
    } else if (value === min.value) {
      min.pure[F]
    } else if (value === max.value) {
      max.pure[F]
    } else {
      new Segments(value) {}.pure[F]
    }
  }

  def opt(value: Int): Option[Segments] = of[Option](value)

  def unsafe[A](value: A)(implicit numeric: Numeric[A]): Segments = of[Id](numeric.toInt(value))

  implicit class SegmentsOps(val self: Segments) extends AnyVal {

    @deprecated("use `metaJournalSegmentNrs` instead", "4.1.0")
    def segmentNrs: List[SegmentNr] = metaJournalSegmentNrs

    def metaJournalSegmentNrs: List[SegmentNr] = SegmentNr.allForSegmentSize(self)
  }
}
