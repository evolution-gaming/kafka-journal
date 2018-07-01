package com.evolutiongaming.kafka.journal

import org.scalatest.{FunSuite, Matchers}
import play.api.libs.json.{Format, JsValue, Json}

class ActionJsonSpec extends FunSuite with Matchers {

  test("Append format") {
    val action: Action = Action.Append(SeqRange(1, 5))
    verify(action)
  }

  test("Truncate format") {
    val action: Action = Action.Truncate(3)
    verify(action)
  }

  test("Read format") {
    val action: Action = Action.Mark("id")
    verify(action)
  }


  private def verify[T](value: T)(implicit format: Format[T]) = {

    def verify(json: JsValue) = {
      val actual = format.reads(json)
      actual.get shouldEqual value
    }

    def verifyAgainstJson() = {
      val json = format.writes(value)
      verify(json)
    }

    def verifyAgainstFile() = {
      val name = value.getClass.getSimpleName
      val stream = value.getClass.getResourceAsStream(s"$name.json")
      val json = Json.parse(stream)
      verify(json)
    }

    verifyAgainstJson()
    verifyAgainstFile()
  }
}
