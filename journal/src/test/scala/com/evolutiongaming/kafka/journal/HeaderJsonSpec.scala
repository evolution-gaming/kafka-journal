package com.evolutiongaming.kafka.journal

import com.evolutiongaming.kafka.journal.Action.Header
import com.evolutiongaming.kafka.journal.HeaderFormats._
import org.scalatest.{FunSuite, Matchers}
import play.api.libs.json.{Format, JsValue, Json}

class HeaderJsonSpec extends FunSuite with Matchers {

  test("Append format") {
    val header: Header = Header.Append(SeqRange(1, 5))
    verify(header)
  }

  test("Delete format") {
    val header: Header = Header.Delete(3)
    verify(header)
  }

  test("Mark format") {
    val header: Header = Header.Mark("id")
    verify(header)
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
      val name = value.getClass
        .getName
        .split('.')
        .filter(_.nonEmpty)
        .last
        .split('$')
        .filter(_.nonEmpty)
        .last
      val stream = value.getClass.getResourceAsStream(s"$name.json")
      val json = Json.parse(stream)
      verify(json)
    }

    verifyAgainstJson()
    verifyAgainstFile()
  }
}
