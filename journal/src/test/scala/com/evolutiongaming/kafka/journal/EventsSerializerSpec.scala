package com.evolutiongaming.kafka.journal

import com.evolutiongaming.kafka.journal.Alias.SeqNr
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.serialization.SerializerHelper._
import org.scalatest.{FunSuite, Matchers}

class EventsSerializerSpec extends FunSuite with Matchers {

  test("toBytes & fromBytes") {

    def event(seqNr: SeqNr) = Event(seqNr, Set.empty, seqNr.toString.getBytes(Utf8))

    val expected = Nel(event(0), event(1), event(2))
    val bytes = EventsSerializer.toBytes(expected)
    val actual = EventsSerializer.fromBytes(bytes)

    (actual.toList zip expected.toList) foreach { case (actual, expected) =>
      actual.seqNr shouldEqual expected.seqNr
      new String(actual.payload, Utf8) shouldEqual new String(expected.payload, Utf8)
    }
  }
}
