package com.evolutiongaming.kafka.journal

import com.evolutiongaming.kafka.journal.Aliases.SeqNr
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.serialization.SerializerHelper._
import org.scalatest.{FunSuite, Matchers}

class EventsSerializerSpec extends FunSuite with Matchers {

  test("toBinary & fromBinary") {

    def event(seqNr: SeqNr) = JournalRecord.Event(seqNr, seqNr.toString.getBytes(Utf8))

    val expected = JournalRecord.Payload.Events(Nel(event(0), event(1), event(2)))
    val bytes = EventsSerializer.EventsToBytes(expected)
    val actual = EventsSerializer.EventsFromBytes(bytes)

    val event0 = event(0)
    actual.copy(events = Nel(event0)) shouldEqual expected.copy(events = Nel(event0))

    (actual.events.toList zip expected.events.toList) foreach { case (actual, expected) =>
      actual.seqNr shouldEqual expected.seqNr
      new String(actual.payload, Utf8) shouldEqual new String(expected.payload, Utf8)
    }
  }
}
