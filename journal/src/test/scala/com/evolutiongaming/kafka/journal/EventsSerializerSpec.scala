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
        event(1, Payload.Binary("payload")))),
      ("text", PayloadType.Json, Nel(
        event(1, Payload.Text(""" {"key":"value"} """)))),
      ("json", PayloadType.Json, Nel(
        event(1, Payload.Json("payload")))),
      ("empty-many", PayloadType.Json, Nel(
        event(1),
        event(2))),
      ("binary-many", PayloadType.Binary, Nel(
        event(1, Payload.Binary("1")),
        event(2, Payload.Binary("2")))),
      ("text-many", PayloadType.Json, Nel(
        event(1, Payload.Text("1")),
        event(2, Payload.Text("2")))),
      ("json-many", PayloadType.Json, Nel(
        event(1, Payload.Json("1")),
        event(2, Payload.Json("2")))),
      ("empty-binary-text-json", PayloadType.Binary, Nel(
        event(1),
        event(2, Payload.Binary("binary")),
        event(3, Payload.Text("text")),
        event(4, Payload.Json("json")))))
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

      val path = s"action.payload.$name.$ext"

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
