package com.evolutiongaming.kafka.journal.eventual


import cats.implicits._
import com.evolutiongaming.kafka.journal._
import org.scalatest.EitherValues
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import play.api.libs.json.{Json => PlayJson}
import scodec.bits.ByteVector

import scala.util.Try

class EventualPayloadAndTypeSpec extends AnyFunSuite with Matchers with EitherValues {

  implicit val jsonCodec: JsonCodec[Try] = JsonCodec.default[Try]

  private val eventualWrite = EventualWrite.summon[Try, Payload]
  private val eventualRead = EventualRead.summon[Try, Payload]

  for {
    (name, payload) <- List(
      ("text", Payload.text("text")),
      ("binary", PayloadBinaryFromStr("binary")),
      ("json", Payload.json("json"))
    )
  } {
    test(s"toEventual & fromEventual, payload: $name") {
      val actual = for {
        payloadAndType <- eventualWrite(payload)
        actual <- eventualRead(payloadAndType)
      } yield actual

      actual shouldBe payload.pure[Try]
    }
  }

  test("toEventual: binary") {
    val payload = PayloadBinaryFromStr("binary")

    val eventual = eventualWrite(payload)

    eventual shouldBe EventualPayloadAndType(payload.value.asRight, PayloadType.Binary).pure[Try]
  }

  test("toEventual: text") {
    val payload = Payload.Text("text")

    val eventual = eventualWrite(payload)

    eventual shouldBe EventualPayloadAndType("text".asLeft, PayloadType.Text).pure[Try]
  }

  test("toEventual: json") {
    val payload = Payload.Json(PlayJson.obj("key" -> "value"))

    val eventual = eventualWrite(payload)

    eventual shouldBe EventualPayloadAndType("""{"key":"value"}""".asLeft, PayloadType.Json).pure[Try]
  }

  test("fromEventual: returns an error for payload type binary and payload string") {
    val payloadAndType = EventualPayloadAndType("text".asLeft, PayloadType.Binary)

    val result = eventualRead(payloadAndType).toEither

    result.left.value shouldBe a[JournalError]
    result.left.value.getMessage should include("Bytes expected")
  }

  test("fromEventual: returns an error for payload type text and payload bytes") {
    val payloadAndType = EventualPayloadAndType(ByteVector.empty.asRight, PayloadType.Text)

    val result = eventualRead(payloadAndType).toEither

    result.left.value shouldBe a[JournalError]
    result.left.value.getMessage should include("String expected")
  }

  test("fromEventual: returns an error for payload type json and payload bytes") {
    val payloadAndType = EventualPayloadAndType(ByteVector.empty.asRight, PayloadType.Json)

    val result = eventualRead(payloadAndType).toEither

    result.left.value shouldBe a[JournalError]
    result.left.value.getMessage should include("String expected")
  }
}
