package com.evolution.kafka.journal

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import play.api.libs.json.JsString
import scodec.bits.ByteVector

class PayloadSpec extends AnyFunSuite with Matchers {

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
