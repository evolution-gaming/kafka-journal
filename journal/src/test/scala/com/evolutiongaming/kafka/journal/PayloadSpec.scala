package com.evolutiongaming.kafka.journal

import org.scalatest.{FunSuite, Matchers}
import play.api.libs.json.JsString
import scodec.bits.ByteVector

class PayloadSpec extends FunSuite with Matchers {

  test("apply text") {
    Payload("text") shouldEqual Payload.text("text")
  }

  test("apply binary") {
    Payload(ByteVector.empty) shouldEqual Payload.binary(ByteVector.empty)
  }

  test("apply json") {
    Payload(JsString("json")) shouldEqual Payload.json(JsString("json"))
  }
}
