package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.kernel.Eq
import cats.syntax.all.*
import cats.{Applicative, Id, Monad, Order, Show}
import com.evolutiongaming.kafka.journal.util.Fail
import com.evolutiongaming.kafka.journal.util.Fail.implicits.*
import com.evolutiongaming.kafka.journal.{Key, SeqNr}
import com.evolutiongaming.scassandra.{DecodeByName, DecodeRow, EncodeByName, EncodeRow}

/**
  * Segment fields in `journal` and `metajournal` tables. Part of clustering key in both tables.
  * 
  * [[SegmentNr]] can be limited: in `metajournal` table cannot be more than [[Segments]] thus:
  * ```
  *    val metaJournalSegmentNr: SegmentNr = ???
  *    val metaJournalSegments: Segments = ???
  *    metaJournalSegmentNr <= metaJournalSegments // have to be true
  * ```
  * 
  * In `journal` table [[SegmentNr]] is not limited but rather each segment contains not more than [[SegmentSize]] events:
  * ```
  *   val journalSegmentNr: SegmentNr = ???
  *   val journalSegmentSize: SegmentSize = ???
  *   journalSegmentNr.value > journalSegmentSize.value // can be true but not have to be
  * ```
  * 
  * Used to partition the `journal` table into segments thus distribute data between Cassandra partitions.
  * For `journal` table [[SegmentNr]] calculated based on [[SeqNr]] and [[SegmentSize]] in [[SegmentNr.journal]]:
  * ```
  *    val seqNr: SeqNr             = SeqNr.unsafe(45629)
  *    val segmentSize: SegmentSize = SegmentSize.default // 10000
  *    val segmentNr: SegmentNr     = SegmentNr.journal(seqNr, segmentSize) // (45629 - 1) / 10000 = 4 where `1` is SeqNr.min
  * ```  
  * 
  * Used to partition the `metajournal` table into segments thus simplify iterating over all records in the table.
  * For `metajournal` table [[SegmentNr]] calculated based on [[Key]] and [[Segments]] in [[SegmentNr.metaJournal]]:
  * ```
  *    val key: Key             = Key("journal ID", "journal topic") // key.id.toLowerCase.hashCode = 969045668
  *    val segments: Segments   = Segments.default // 10000
  *    val segmentNr: SegmentNr = SegmentNr.metaJournal(key, segments) // Math.abs("journal ID".toLowerCase.hashCode) % 10000 = 5668
  * ``` 
  * 
  * @see [[Segments]] for `metajournal` table
  * @see [[SegmentNrs]] for `metajournal` table
  * @see [[SegmentOf]] for `metajournal` table
  * @see [[SegmentNrsOf]] for `metajournal` table
  * 
  * @see [[SegmentSize]] for `journal` table
  * @see [[Segment]] for `journal` table
  */
sealed abstract case class SegmentNr(value: Long) {

  override def toString: String = value.toString
}

object SegmentNr {

  /**
    * [[SegmentNr]] factory.
    */
  sealed trait Of[F[_]] {

    /**
      * Calculate [[SegmentNr]] for `metajournal` table.
      * 
      * @see [[SegmentNr.metaJournal]] for the actual algorithm.
      */
    def metaJournal(key: Key): F[SegmentNr]
  }

  object Of {
    def const[F[_]: Applicative](nr: SegmentNr): Of[F] =
      new Of[F] {
        def metaJournal(key: Key): F[SegmentNr] = nr.pure[F]
      }

    /**
      * Use [[Segments]] to calculate [[SegmentNr]] for `metajournal` table. 
      *
      * @param segments total number of segments in `metajournal` table
      * @return [[SegmentNr.Of]] factory
      */
    def apply[F[_]: Applicative](segments: Segments): Of[F] =
      new Of[F] {
        def metaJournal(key: Key): F[SegmentNr] = SegmentNr.metaJournal(key, segments).pure[F]
      }
  }

  val min: SegmentNr = new SegmentNr(0L) {}

  val max: SegmentNr = new SegmentNr(Long.MaxValue) {}

  implicit val eqSegmentNr: Eq[SegmentNr] = Eq.fromUniversalEquals

  implicit val showSegmentNr: Show[SegmentNr] = Show.fromToString

  implicit val orderingSegmentNr: Ordering[SegmentNr] = Ordering.by(_.value)

  implicit val orderSegmentNr: Order[SegmentNr] = Order.fromOrdering

  implicit val encodeByNameSegmentNr: EncodeByName[SegmentNr] = EncodeByName[Long].contramap(_.value)

  implicit val decodeByNameSegmentNr: DecodeByName[SegmentNr] = DecodeByName[Long].map { a => SegmentNr.of[Id](a) }

  implicit val encodeRowSegmentNr: EncodeRow[SegmentNr] = EncodeRow[SegmentNr]("segment")

  implicit val decodeRowSegmentNr: DecodeRow[SegmentNr] = DecodeRow[SegmentNr]("segment")

  def of[F[_]: Applicative: Fail](value: Long): F[SegmentNr] = {
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
  }

  @deprecated("use `journal` instead", "4.1.0")
  def apply(seqNr: SeqNr, segmentSize: SegmentSize): SegmentNr = journal(seqNr, segmentSize)

  /**
    * Calculate segment number for `journal` table
    */
  def journal(seqNr: SeqNr, segmentSize: SegmentSize): SegmentNr = {
    val segmentNr = (seqNr.value - SeqNr.min.value) / segmentSize.value
    new SegmentNr(segmentNr) {}
  }

  @deprecated("use `metaJournal` instead", "4.1.0")
  def apply(hashCode: Int, segments: Segments): SegmentNr = {
    val segmentNr = math.abs(hashCode.toLong % segments.value)
    new SegmentNr(segmentNr) {}
  }

  /**
    * Calculate segment number for `metajournal` table
    */
  def metaJournal(key: Key, segments: Segments): SegmentNr = {
    val hashCode  = key.id.toLowerCase.hashCode
    val segmentNr = math.abs(hashCode.toLong % segments.value)
    new SegmentNr(segmentNr) {}
  }

  def opt(value: Long): Option[SegmentNr] = of[Option](value)

  def unsafe[A](value: A)(implicit numeric: Numeric[A]): SegmentNr = of[Id](numeric.toLong(value))

  @deprecated("use `allForSegmentSize` instead", "4.1.0")
  def fromSegments(segments: Segments): List[SegmentNr] = {
    allForSegmentSize(segments)
  }

  /**
    * All possible [[SegmentNr]] values for the given [[Segments]].
    */
  def allForSegmentSize(segments: Segments): List[SegmentNr] = {
    min
      .value
      .until(segments.value.toLong)
      .map { a => new SegmentNr(a) {} }
      .toList
  }

  implicit class SegmentNrOps(val self: SegmentNr) extends AnyVal {

    def to[F[_]: Monad: Fail](segmentNr: SegmentNr): F[List[SegmentNr]] = {
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
                .map { prev => (prev, segmentNr :: segmentNrs).asLeft[List[SegmentNr]] }
            }
        }
      }
    }

    def map[F[_]: Applicative: Fail](f: Long => Long): F[SegmentNr] = SegmentNr.of[F](f(self.value))

    def next[F[_]: Applicative: Fail]: F[SegmentNr] = map { _ + 1L }

    def prev[F[_]: Applicative: Fail]: F[SegmentNr] = map { _ - 1L }
  }

  object implicits {
    implicit class SeqNrOpsSegmentNr(val self: SeqNr) extends AnyVal {

      @deprecated("use `seqNr.toJournalSegmentNr` instead", "4.1.0")
      def toSegmentNr(segmentSize: SegmentSize): SegmentNr = SegmentNr.journal(self, segmentSize)

      def toJournalSegmentNr(segmentSize: SegmentSize): SegmentNr = SegmentNr.journal(self, segmentSize)
    }
  }
}
