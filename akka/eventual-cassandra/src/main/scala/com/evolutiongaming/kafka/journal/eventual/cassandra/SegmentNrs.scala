package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.Applicative
import cats.syntax.all.*
import com.evolutiongaming.kafka.journal.Key

/**
 * Contains segments that query should be performed in.
 *
 * The query for both segments may be performed in parallel, but the result of the first segment is
 * to be preferred.
 *
 * The class is meant to allow increasing number of segments in backwards compatible manner.
 *
 * @param first
 *   segment to be preferred if data is found
 * @param second
 *   segment is to use if data for `first` is not found
 */
private[journal] sealed abstract case class SegmentNrs(first: SegmentNr, second: Option[SegmentNr])

private[journal] object SegmentNrs {

  sealed trait Of[F[_]] {
    def metaJournal(key: Key): F[SegmentNrs]
  }

  object Of {

    def const[F[_]: Applicative](nrs: SegmentNrs): Of[F] = new Of[F] {
      def metaJournal(key: Key) = nrs.pure[F]
    }

    def apply[F[_]: Applicative](first: Segments, second: Segments): Of[F] = new Of[F] {
      def metaJournal(key: Key) =
        SegmentNrs(
          first = SegmentNr.metaJournal(key, first),
          second = SegmentNr.metaJournal(key, second),
        ).pure[F]
    }

  }

  def apply(first: SegmentNr, second: SegmentNr): SegmentNrs = {
    new SegmentNrs(first, if (first == second) none else second.some) {}
  }

  def apply(segmentNr: SegmentNr): SegmentNrs = {
    new SegmentNrs(segmentNr, none) {}
  }
}
