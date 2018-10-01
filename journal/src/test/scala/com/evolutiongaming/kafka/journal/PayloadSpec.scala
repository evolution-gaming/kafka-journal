package com.evolutiongaming.kafka.journal

import org.scalatest.{FunSuite, Matchers}
import play.api.libs.json.JsString

class PayloadSpec extends FunSuite with Matchers {

  test("apply text") {
    Payload("text") shouldEqual Payload.Text("text")
  }

  test("apply binary") {
    Payload(Bytes.Empty) shouldEqual Payload.Binary(Bytes.Empty)
  }

  test("apply json") {
    Payload(JsString("json")) shouldEqual Payload.Json(JsString("json"))
  }

  test("toString") {
    Payload.Binary("test").toString shouldEqual "Binary(4)"
  }
}
