package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.Id
import com.evolutiongaming.kafka.journal.Key
import org.scalatest.{FunSuite, Matchers}

class SegmentNrOfTest extends FunSuite with Matchers {

  for {
    (segments, segmentNr) <- List(
      (Segments.min,     SegmentNr.min),
      (Segments.default, SegmentNr.unsafe(55)),
      (Segments.max,     SegmentNr.unsafe(3355)))
  } yield {
    test(s"segments: $segments, segmentNr: $segmentNr") {
      val segmentNrOf = SegmentNrOf[Id](segments)
      val key = Key(id = "id", topic = "topic")
      segmentNrOf(key) shouldEqual segmentNr
    }
  }
}
