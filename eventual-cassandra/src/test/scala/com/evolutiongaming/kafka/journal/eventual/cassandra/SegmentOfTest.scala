package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.Id
import com.evolutiongaming.kafka.journal.Key
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class SegmentOfTest extends AnyFunSuite with Matchers {

  for {
    id <- List("id", "ID")
    (segments, segmentNr) <- List(
      (Segments.min, SegmentNr.min),
      (Segments.old, SegmentNr.unsafe(55)),
      (Segments.max, SegmentNr.unsafe(3355)),
    )
  } yield {
    test(s"id: $id, segments: $segments, segmentNr: $segmentNr") {
      val segmentOf = SegmentNr.Of[Id](segments)
      val key = Key(id = id, topic = "topic")
      segmentOf.metaJournal(key) shouldEqual segmentNr
    }
  }
}
