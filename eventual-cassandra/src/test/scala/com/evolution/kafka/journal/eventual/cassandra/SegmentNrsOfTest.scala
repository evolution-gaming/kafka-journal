package com.evolution.kafka.journal.eventual.cassandra

import cats.Id
import com.evolution.kafka.journal.Key
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class SegmentNrsOfTest extends AnyFunSuite with Matchers {
  for {
    id <- List("id", "ID")
    (first, second, segmentNrs) <- List(
      (Segments.min, Segments.min, SegmentNrs(SegmentNr.min)),
      (Segments.old, Segments.old, SegmentNrs(SegmentNr.unsafe(55))),
      (Segments.max, Segments.max, SegmentNrs(SegmentNr.unsafe(3355))),
      (Segments.min, Segments.max, SegmentNrs(SegmentNr.min, SegmentNr.unsafe(3355))),
      (Segments.min, Segments.old, SegmentNrs(SegmentNr.min, SegmentNr.unsafe(55))),
      (Segments.old, Segments.max, SegmentNrs(SegmentNr.unsafe(55), SegmentNr.unsafe(3355))),
    )
  } yield {
    test(s"id: $id, first: $first, second: $second, segmentNrs: $segmentNrs") {
      val segmentNrsOf = SegmentNrs.Of[Id](first, second)
      val key = Key(id = id, topic = "topic")
      segmentNrsOf.metaJournal(key) shouldEqual segmentNrs
    }
  }
}
