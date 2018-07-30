package com.evolutiongaming.kafka.journal

import com.evolutiongaming.nel.Nel
import com.evolutiongaming.serialization.SerializerHelper._
import org.scalatest.{FunSuite, Matchers}

class EventsSerializerSpec extends FunSuite with Matchers {
  import EventsSerializerSpec._
  

  test("toBytes & fromBytes") {

    def event(seqNr: Long) = Event(SeqNr(seqNr), Set.empty, Bytes(seqNr.toString.getBytes(Utf8)))

    val expected = Nel(event(1), event(2), event(3))
    val bytes = EventsSerializer.toBytes(expected)
    val actual = EventsSerializer.fromBytes(bytes)

    (actual.toList zip expected.toList) foreach { case (actual, expected) =>
      actual.seqNr shouldEqual expected.seqNr
      actual.payload.str shouldEqual expected.payload.str
    }
  }
}

object EventsSerializerSpec {
  implicit class BytesOps(val self: Bytes) extends AnyVal {
    def str: String = new String(self.value, Utf8)
  }
}
