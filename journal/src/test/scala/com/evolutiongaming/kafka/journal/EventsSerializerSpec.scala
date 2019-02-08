package com.evolutiongaming.kafka.journal

import java.io.FileOutputStream

import com.evolutiongaming.kafka.journal.EventsSerializer.{EventsFromPayload, EventsToPayload}
import com.evolutiongaming.kafka.journal.FixEquality.Implicits._
import com.evolutiongaming.nel.Nel
import org.scalatest.{FunSuite, Matchers}

class EventsSerializerSpec extends FunSuite with Matchers {

  private implicit val fixEquality = FixEquality.array[Byte]()

  def event(seqNr: Int, payload: Option[Payload] = None): Event = {
    val tags = (0 to seqNr).map(_.toString).toSet
    Event(SeqNr(seqNr.toLong), tags, payload)
  }

  def event(seqNr: Int, payload: Payload): Event = {
    event(seqNr, Some(payload))
  }

  for {
    (name, payloadType, events) <- List(
      ("empty", PayloadType.Json, Nel(
        event(1))),
      ("binary", PayloadType.Binary, Nel(
        event(1, Payload.binary("payload")))),
      ("text", PayloadType.Json, Nel(
        event(1, Payload.text(""" {"key":"value"} """)))),
      ("json", PayloadType.Json, Nel(
        event(1, Payload.json("payload")))),
      ("empty-many", PayloadType.Json, Nel(
        event(1),
        event(2))),
      ("binary-many", PayloadType.Binary, Nel(
        event(1, Payload.binary("1")),
        event(2, Payload.binary("2")))),
      ("text-many", PayloadType.Json, Nel(
        event(1, Payload.text("1")),
        event(2, Payload.text("2")))),
      ("json-many", PayloadType.Json, Nel(
        event(1, Payload.json("1")),
        event(2, Payload.json("2")))),
      ("empty-binary-text-json", PayloadType.Binary, Nel(
        event(1),
        event(2, Payload.binary("binary")),
        event(3, Payload.text("text")),
        event(4, Payload.json("json")))))
  } {

    test(s"toBytes & fromBytes, events: $name") {

      def fromFile(path: String) = BytesOf(getClass, path)

      def verify(payload: Bytes, payloadType: PayloadType.BinaryOrJson) = {
        val actual = EventsFromPayload(Payload.Binary(payload), payloadType)
        actual.fix shouldEqual events.fix
      }

      val (payload, payloadTypeActual) = EventsToPayload(events)

      payloadType shouldEqual payloadTypeActual

      val ext = payloadType.ext

      val path = s"Payload-$name.$ext"

      //      writeToFile(payload.value, path)

      verify(payload.value, payloadType)

      verify(fromFile(path), payloadType)
    }
  }

  def writeToFile(bytes: Bytes, path: String): Unit = {
    val os = new FileOutputStream(path)
    os.write(bytes)
    os.close()
  }
}
