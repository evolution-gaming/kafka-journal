package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.syntax.all.*

/** Contains segments that query should be performed in.
  *
  * The query for both segments may be performed in parallel, but
  * the result of the first segment is to be preferred.
  *
  * The class is meant to allow increasing number of segments in
  * backwards compatible manner.
  *
  * @param first segment to be preferred if data is found
  * @param second segment is to use if data for `first` is not found
  */
sealed abstract case class SegmentNrs(first: SegmentNr, second: Option[SegmentNr])

object SegmentNrs {

  def apply(first: SegmentNr, second: SegmentNr): SegmentNrs = {
    new SegmentNrs(first, if (first == second) none else second.some) {}
  }

  def apply(segmentNr: SegmentNr): SegmentNrs = {
    new SegmentNrs(segmentNr, none) {}
  }
}
