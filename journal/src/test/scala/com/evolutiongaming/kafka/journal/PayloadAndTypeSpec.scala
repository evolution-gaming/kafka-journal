package com.evolutiongaming.kafka.journal

import java.io.FileOutputStream

import cats.data.{NonEmptyList => Nel}
import cats.implicits._
import com.evolutiongaming.kafka.journal.conversions.{EventsToPayload, PayloadToEvents}
import com.evolutiongaming.kafka.journal.ExpireAfter.implicits._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import play.api.libs.json.Json
import scodec.bits.ByteVector

import scala.util.Try
import scala.concurrent.duration._

class PayloadAndTypeSpec extends AnyFunSuite with Matchers {

  def event(seqNr: Int, payload: Option[Payload] = None): Event = {
    val tags = (0 to seqNr).map(_.toString).toSet
    Event(SeqNr.unsafe(seqNr), tags, payload)
  }

  def event(seqNr: Int, payload: Payload): Event = {
    event(seqNr, payload.some)
  }

  def binary(a: String): Payload.Binary = PayloadBinaryFromStr(a)

  private implicit val fromAttempt = FromAttempt.lift[Try]
  
  private implicit val fromJsResult = FromJsResult.lift[Try]

  private val payloadMetadata = PayloadMetadata(
    1.day.toExpireAfter.some,
    Json.obj(("key", "value")).some)

  for {
    (name, payloadType, events) <- List(
      ("empty", PayloadType.Json, Events(
        Nel.of(
          event(1)),
        PayloadMetadata.empty)),
      ("binary", PayloadType.Binary, Events(
        Nel.of(
          event(1, binary("payload"))),
        PayloadMetadata.empty)),
      ("text", PayloadType.Json, Events(
        Nel.of(
          event(1, Payload.text(""" {"key":"value"} """))),
        PayloadMetadata.empty)),
      ("json", PayloadType.Json, Events(
        Nel.of(
          event(1, Payload.json("payload"))),
        PayloadMetadata.empty)),
      ("empty-many", PayloadType.Json, Events(
        Nel.of(
          event(1),
          event(2)),
        payloadMetadata)),
      ("binary-many", PayloadType.Binary, Events(
        Nel.of(
          event(1, binary("1")),
          event(2, binary("2"))),
        payloadMetadata)),
      ("text-many", PayloadType.Json, Events(
        Nel.of(
          event(1, Payload.text("1")),
          event(2, Payload.text("2"))),
        payloadMetadata)),
      ("json-many", PayloadType.Json, Events(
        Nel.of(
          event(1, Payload.json("1")),
          event(2, Payload.json("2"))),
        payloadMetadata)),
      ("empty-binary-text-json", PayloadType.Binary, Events(
        Nel.of(
          event(1),
          event(2, binary("binary")),
          event(3, Payload.text("text")),
          event(4, Payload.json("json"))),
        payloadMetadata)))
  } {

    test(s"toBytes & fromBytes, events: $name") {

      def fromFile(path: String) = ByteVectorOf[Try](getClass, path).get

      def verify(payload: ByteVector, payloadType: PayloadType.BinaryOrJson) = {
        val payloadAndType = PayloadAndType(payload, payloadType)
        val payloadToEvents = PayloadToEvents[Try]
        payloadToEvents(payloadAndType) shouldEqual events.pure[Try]
      }

      val eventsToPayload = EventsToPayload[Try]
      val payloadAndType = eventsToPayload(events).get

      payloadType shouldEqual payloadAndType.payloadType

      val ext = payloadType.ext

      val path = s"Payload-$name.$ext"

//      writeToFile(payloadAndType.payload, path)

      verify(payloadAndType.payload, payloadType)

      verify(fromFile(path), payloadType)
    }
  }

  def writeToFile(bytes: ByteVector, path: String): Unit = {
    val os = new FileOutputStream(path)
    os.write(bytes.toArray)
    os.close()
  }
}
