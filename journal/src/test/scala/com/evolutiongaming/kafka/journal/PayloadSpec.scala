package com.evolutiongaming.kafka.journal

import org.scalatest.{FunSuite, Matchers}
import play.api.libs.json.JsString

class PayloadSpec extends FunSuite with Matchers {

  test("apply text") {
    Payload("text") shouldEqual Payload.text("text")
  }

  test("apply binary") {
    Payload(Bytes.Empty) shouldEqual Payload.binary(Bytes.Empty)
  }

  test("apply json") {
    Payload(JsString("json")) shouldEqual Payload.json(JsString("json"))
  }

  test("toString") {
    Payload.binary("test").toString shouldEqual "Binary(4)"
  }
}
