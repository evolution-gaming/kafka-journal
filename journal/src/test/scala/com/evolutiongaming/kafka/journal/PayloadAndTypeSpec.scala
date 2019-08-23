package com.evolutiongaming.kafka.journal

import java.io.FileOutputStream

import cats.data.{NonEmptyList => Nel}
import com.evolutiongaming.kafka.journal.PayloadAndType.{EventsFromPayload, EventsToPayload}
import org.scalatest.{FunSuite, Matchers}
import scodec.bits.ByteVector

class PayloadAndTypeSpec extends FunSuite with Matchers {

  def event(seqNr: Int, payload: Option[Payload] = None): Event = {
    val tags = (0 to seqNr).map(_.toString).toSet
    Event(SeqNr(seqNr.toLong), tags, payload)
  }

  def event(seqNr: Int, payload: Payload): Event = {
    event(seqNr, Some(payload))
  }

  def binary(a: String) = PayloadBinaryFromStr(a)

  for {
    (name, payloadType, events) <- List(
      ("empty", PayloadType.Json, Nel.of(
        event(1))),
      ("binary", PayloadType.Binary, Nel.of(
        event(1, binary("payload")))),
      ("text", PayloadType.Json, Nel.of(
        event(1, Payload.text(""" {"key":"value"} """)))),
      ("json", PayloadType.Json, Nel.of(
        event(1, Payload.json("payload")))),
      ("empty-many", PayloadType.Json, Nel.of(
        event(1),
        event(2))),
      ("binary-many", PayloadType.Binary, Nel.of(
        event(1, binary("1")),
        event(2, binary("2")))),
      ("text-many", PayloadType.Json, Nel.of(
        event(1, Payload.text("1")),
        event(2, Payload.text("2")))),
      ("json-many", PayloadType.Json, Nel.of(
        event(1, Payload.json("1")),
        event(2, Payload.json("2")))),
      ("empty-binary-text-json", PayloadType.Binary, Nel.of(
        event(1),
        event(2, binary("binary")),
        event(3, Payload.text("text")),
        event(4, Payload.json("json")))))
  } {

    test(s"toBytes & fromBytes, events: $name") {

      def fromFile(path: String) = ByteVector.view(BytesOf(getClass, path))

      def verify(payload: ByteVector, payloadType: PayloadType.BinaryOrJson) = {
        val payloadAndType = PayloadAndType(payload, payloadType)
        val actual = EventsFromPayload(payloadAndType)
        actual shouldEqual events
      }

      val payloadAndType = EventsToPayload(events)

      payloadType shouldEqual payloadAndType.payloadType

      val ext = payloadType.ext

      val path = s"Payload-$name.$ext"

      //      writeToFile(payload.value, path)

      verify(payloadAndType.payload, payloadType)

      verify(fromFile(path), payloadType)
    }
  }

  def writeToFile(bytes: Bytes, path: String): Unit = {
    val os = new FileOutputStream(path)
    os.write(bytes)
    os.close()
  }
}
