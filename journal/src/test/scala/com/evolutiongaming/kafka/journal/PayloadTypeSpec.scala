package com.evolutiongaming.kafka.journal

import org.scalatest.{FunSuite, Matchers}
import play.api.libs.json.JsString

class PayloadTypeSpec extends FunSuite with Matchers {

  for {
    (ext, payloadType) <- List(
      ("json", PayloadType.Json),
      ("txt", PayloadType.Text),
      ("bin", PayloadType.Binary))
  } {
    test(s"$payloadType.ext") {
      payloadType.ext shouldEqual ext
    }
  }

  for {
    (json, expected) <- List(
      ("json", Some(PayloadType.Json)),
      ("text", Some(PayloadType.Text)),
      ("binary", Some(PayloadType.Binary)),
      ("none", None))
  } {
    test(s"reads & writes $json") {
      JsString(json).validate[PayloadType].asOpt shouldEqual expected
    }
  }

  for {
    (json, expected) <- List(
      ("json", Some(PayloadType.Json)),
      ("text", Some(PayloadType.Text)),
      ("binary", None))
  } {
    test(s"TextOrJson reads & writes $json") {
      JsString(json).validate[PayloadType.TextOrJson].asOpt shouldEqual expected
    }
  }

  for {
    (json, expected) <- List(
      ("json", Some(PayloadType.Json)),
      ("text", None),
      ("binary", Some(PayloadType.Binary)))
  } {
    test(s"BinaryOrJson reads & writes $json") {
      JsString(json).validate[PayloadType.BinaryOrJson].asOpt shouldEqual expected
    }
  }
}
