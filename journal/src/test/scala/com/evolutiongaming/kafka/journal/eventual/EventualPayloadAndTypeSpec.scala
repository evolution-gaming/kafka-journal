package com.evolutiongaming.kafka.journal.eventual

import java.nio.charset.StandardCharsets

import cats.implicits._
import com.evolutiongaming.kafka.journal.{JsonCodec, Payload, PayloadBinaryFromStr, PayloadType}
import org.scalatest.EitherValues
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import play.api.libs.json.{Json => PlayJson}
import scodec.bits.ByteVector

import scala.util.{Success, Try}

class EventualPayloadAndTypeSpec extends AnyFunSuite with Matchers with EitherValues {

  implicit val jsonCodec: JsonCodec[Try] = JsonCodec.default[Try]

  private val eventualWrite = EventualWrite[Try, Payload]
  private val eventualRead = EventualRead[Try, Payload]

  for {
    (name, payload) <- List(
      ("text", Payload.text("text")),
      ("binary", PayloadBinaryFromStr("binary")),
      ("json", Payload.json("json"))
    )
  } {
    test(s"toEventual & fromEventual, payload: $name") {
      val eventual = eventualWrite.writeEventual(payload).get

      eventualRead.readEventual(eventual) shouldBe Success(payload)
    }
  }

  test("toEventual: binary") {
    val payload = PayloadBinaryFromStr("binary")

    val eventual = eventualWrite.writeEventual(payload).get

    eventual shouldBe EventualPayloadAndType(Right(payload.value), PayloadType.Binary)
  }

  test("toEventual: text") {
    val payload = Payload.Text("text")

    val eventual = eventualWrite.writeEventual(payload).get

    eventual shouldBe EventualPayloadAndType(Left("text"), PayloadType.Text)
  }

  test("toEventual: json") {
    val payload = Payload.Json(PlayJson.obj("key" -> "value"))

    val eventual = eventualWrite.writeEventual(payload).get

    eventual shouldBe EventualPayloadAndType(Left("""{"key":"value"}"""), PayloadType.Json)
  }
}
