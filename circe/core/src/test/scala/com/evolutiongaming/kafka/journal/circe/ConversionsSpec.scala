package com.evolutiongaming.kafka.journal.circe

import cats.syntax.all._
import org.scalatest.funsuite.AnyFunSuite
import play.api.libs.json._
import org.scalatest.matchers.should.Matchers

class ConversionsSpec extends AnyFunSuite with Matchers {

  for {
    (name, playJson) <- List(
      ("null", JsNull),
      ("string", JsString("string")),
      ("integer number", JsNumber(123)),
      ("decimal number", JsNumber(BigDecimal("1.2345678"))),
      ("true", JsBoolean(true)),
      ("false", JsBoolean(false)),
      ("empty array", JsArray()),
      ("array", JsArray(List(JsString("1"), JsString("2")))),
      ("array of arrays", JsArray(List(
        JsArray(List(JsString("1"), JsString("2"))),
        JsArray(List(JsString("3"), JsString("4")))
      ))),
      ("object", JsObject(List(
        "key1" -> JsNumber(1),
        "key2" -> JsString("2")
      ))),
      ("array of objects", JsArray(List(
        JsObject(List("key" -> JsString("1"))),
        JsObject(List("key" -> JsString("2")))
      )))
    )
  } {
    test(s"toCirce & fromCirce, json: $name") {
      val circeJson = convertPlayToCirce(playJson)
      val fromCirce = convertCirceToPlay(circeJson)

      fromCirce shouldEqual playJson.asRight[String]
    }
  }
}
