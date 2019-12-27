package com.evolutiongaming.kafka.journal

import cats.data.{NonEmptyList => Nel}
import cats.implicits._
import com.evolutiongaming.kafka.journal.util.ScodecHelper.{nelCodec, _}
import org.scalatest.FunSuite
import org.scalatest.matchers.should.Matchers
import scodec.bits.ByteVector
import scodec.{Attempt, Codec, codecs}

class EventsTest extends FunSuite with Matchers {

  test("decode newer version") {
    val codec = {
      val eventsCodec = nelCodec(codecs.listOfN(codecs.int32, codecs.variableSizeBytes(codecs.int32, Codec[Event])))
      val version = ByteVector.fromByte(100)
      (codecs.constant(version) ~> eventsCodec)
        .xmap[Events](a => Events(a, PayloadMetadata.empty), _.events)
    }

    val events = Events(Nel.of(Event(SeqNr.min)), PayloadMetadata.empty)
    val actual = for {
      bits   <- codec.encode(events)
      result <- Events.codecEvents.decode(bits)
    } yield {
      result.value
    }
    actual shouldEqual events.pure[Attempt]
  }
}
