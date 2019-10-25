package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.Id
import com.evolutiongaming.kafka.journal.Key
import org.scalatest.{FunSuite, Matchers}

class SegmentOfTest extends FunSuite with Matchers {

  for {
    id                    <- List("id", "ID")
    (segments, segmentNr) <- List(
      (Segments.min,     SegmentNr.min),
      (Segments.default, SegmentNr.unsafe(55)),
      (Segments.max,     SegmentNr.unsafe(3355)))
  } yield {
    test(s"id: $id, segments: $segments, segmentNr: $segmentNr") {
      val segmentOf = SegmentOf[Id](segments)
      val key = Key(id = id, topic = "topic")
      segmentOf(key) shouldEqual segmentNr
    }
  }
}
