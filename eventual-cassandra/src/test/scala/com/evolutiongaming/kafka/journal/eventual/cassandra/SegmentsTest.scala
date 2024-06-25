package com.evolutiongaming.kafka.journal.eventual.cassandra

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class SegmentsTest extends AnyFunSuite with Matchers {
  for {
    (segments, expected) <- List(
      (Segments.min, List(0)),
      (Segments.old, 0.until(100).toList),
      (Segments.default, 0.until(10000).toList),
    )
  }
    test(s"$segments.segmentNrs") {
      segments.segmentNrs.map(_.value) shouldEqual expected
    }
}
