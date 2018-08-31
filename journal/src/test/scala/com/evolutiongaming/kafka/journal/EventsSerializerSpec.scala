package com.evolutiongaming.kafka.journal

import com.evolutiongaming.nel.Nel
import com.evolutiongaming.serialization.SerializerHelper._
import org.scalatest.{FunSuite, Matchers}

class EventsSerializerSpec extends FunSuite with Matchers {
  import EventsSerializerSpec._


  test("toBytes & fromBytes") {

    def event(seqNr: Long) = {
      val tags = (0l to seqNr).map(_.toString).toSet
      val bytes = Bytes(seqNr.toString.getBytes(Utf8))
      Event(SeqNr(seqNr), tags, bytes)
    }

    val expected = Nel(event(1), event(2), event(3))
    val bytes = EventsSerializer.toBytes(expected)
    bytes.value.length shouldEqual 112
    val actual = EventsSerializer.fromBytes(bytes)

    (actual.toList zip expected.toList) foreach { case (actual, expected) =>
      actual.seqNr shouldEqual expected.seqNr
      actual.payload.str shouldEqual expected.payload.str
      actual.tags shouldEqual expected.tags
    }
  }
}

object EventsSerializerSpec {
  implicit class BytesOps(val self: Bytes) extends AnyVal {
    def str: String = new String(self.value, Utf8)
  }
}
