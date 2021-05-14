package com.evolutiongaming.kafka.journal

import org.scalatest.matchers.should.Matchers
import play.api.libs.json.{JsString, Json}
import scodec.bits.ByteVector

import scala.util.{Failure, Try}
import org.scalatest.funsuite.AnyFunSuite

class JsonCodecTest extends AnyFunSuite with Matchers {

  private val malformed = ByteVector.view(Json.toBytes(JsString("\ud83d\ude18\ud83d")))

  test("JsonCodec.jsoniter") {

    JsonCodec.jsoniter[Try].decode.fromBytes(malformed) should matchPattern {
      case Failure(_: JournalError) =>
    }
  }

  test("JsonCodec.playJson") {
    JsonCodec.playJson[Try].decode.fromBytes(malformed).isSuccess shouldEqual true
  }

  test("JsonCodec.default") {
    JsonCodec.default[Try].decode.fromBytes(malformed).isSuccess shouldEqual true
  }
}
