package com.evolutiongaming.kafka.journal.circe

import cats.syntax.all.*
import com.evolutiongaming.kafka.journal.TestJsonCodec.instance
import com.evolutiongaming.kafka.journal.*
import com.evolutiongaming.kafka.journal.circe.Instances.*
import com.evolutiongaming.kafka.journal.eventual.*
import io.circe.Json as CirceJson
import org.scalatest.EitherValues
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import play.api.libs.json.Json as PlayJson
import scodec.bits.ByteVector

import scala.util.Try

class EventualPayloadAndTypeSpec extends AnyFunSuite with Matchers with EitherValues {

  private val playEventualWrite = EventualWrite.summon[Try, Payload]
  private val circeEventualRead = EventualRead.summon[Try, CirceJson]

  for {
    (playPayload, circePayload) <- List(
      (Payload.json(PlayJson.obj(("key", "value"))), CirceJson.obj("key" -> CirceJson.fromString("value"))),
    )
  } {
    test(s"toEventual with Play, fromEventual with Circe") {
      val actual = for {
        payloadAndType <- playEventualWrite(playPayload)
        actual         <- circeEventualRead(payloadAndType)
      } yield actual

      actual shouldBe circePayload.pure[Try]
    }
  }

  for {
    (name, payloadAndType) <- List(
      ("binary", EventualPayloadAndType(ByteVector.empty.asRight, PayloadType.Binary)),
      ("text", EventualPayloadAndType("text".asLeft, PayloadType.Text)),
    )
  } {
    test(s"fromEventual: returns an error for non-json payload type: $name") {
      val result = circeEventualRead(payloadAndType).toEither

      result.left.value shouldBe a[JournalError]
      result.left.value.getMessage should include(payloadAndType.payloadType.toString)
    }
  }

  test("fromEventual: returns an error for payload type json and payload bytes") {
    val payloadAndType = EventualPayloadAndType(ByteVector.empty.asRight, PayloadType.Json)

    val result = circeEventualRead(payloadAndType).toEither

    result.left.value shouldBe a[JournalError]
    result.left.value.getMessage should include("String expected")
  }

  test("fromEventual: returns an error for malformed json") {
    val malformed      = "{\"key\": {sss}}"
    val payloadAndType = EventualPayloadAndType(malformed.asLeft, PayloadType.Json)

    val result = circeEventualRead(payloadAndType).toEither

    result.left.value shouldBe a[JournalError]
    result.left.value.getMessage should (include("ParsingFailure") and include("sss"))
  }

}
