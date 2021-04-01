package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.syntax.all._


sealed abstract case class SegmentNrs(first: SegmentNr, second: Option[SegmentNr])

object SegmentNrs {

  def apply(first: SegmentNr, second: SegmentNr): SegmentNrs = {
    new SegmentNrs(first, if (first == second) none else second.some) {}
  }

  def apply(segmentNr: SegmentNr): SegmentNrs = {
    new SegmentNrs(segmentNr, none) {}
  }
}