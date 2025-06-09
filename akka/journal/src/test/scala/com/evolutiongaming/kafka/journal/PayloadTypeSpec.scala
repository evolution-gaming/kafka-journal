package com.evolutiongaming.kafka.journal

import cats.syntax.all.*
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import play.api.libs.json.JsString

class PayloadTypeSpec extends AnyFunSuite with Matchers {

  for {
    (json, expected) <- List(
      ("json", PayloadType.Json.some),
      ("text", PayloadType.Text.some),
      ("binary", PayloadType.Binary.some),
      ("none", none),
    )
  } {
    test(s"reads & writes $json") {
      JsString(json).validate[PayloadType].asOpt shouldEqual expected
    }
  }

  for {
    (json, expected) <- List(("json", PayloadType.Json.some), ("text", PayloadType.Text.some), ("binary", none))
  } {
    test(s"TextOrJson reads & writes $json") {
      JsString(json).validate[PayloadType.TextOrJson].asOpt shouldEqual expected
    }
  }

  for {
    (json, expected) <- List(("json", PayloadType.Json.some), ("text", none), ("binary", PayloadType.Binary.some))
  } {
    test(s"BinaryOrJson reads & writes $json") {
      JsString(json).validate[PayloadType.BinaryOrJson].asOpt shouldEqual expected
    }
  }
}
