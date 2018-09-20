package com.evolutiongaming.kafka.journal

import com.evolutiongaming.kafka.journal.SeqNr.Helper._
import org.scalatest.{FunSuite, Matchers}
import play.api.libs.json._

class ActionHeaderJsonSpec extends FunSuite with Matchers {

  for {
    origin <- List(Some(Origin("origin")), None)
  } {
    val variance = s"origin=$origin"

    test(s"Append format, $variance") {
      val range = SeqRange(1, 5)
      val header = ActionHeader.Append(range, origin)
      verify(header, s"Append.$variance")
    }

    test(s"Delete format, $variance") {
      val seqNr = 3.toSeqNr
      val header = ActionHeader.Delete(seqNr, origin)
      verify(header, s"Delete.$variance")
    }

    test(s"Mark format, $variance") {
      val header = ActionHeader.Mark("id", origin)
      verify(header, s"Mark.$variance")
    }
  }

  private def verify(value: ActionHeader, name: String) = {

    def verify(json: JsValue) = {
      val actual = json.as[ActionHeader]
      actual shouldEqual value
    }

    def verifyAgainstJson() = {
      val json = Json.toJson(value)
      verify(json)
    }

    def verifyAgainstFile() = {
      val path = s"$name.json"
      val stream = Option(getClass.getResourceAsStream(path)) getOrElse {
        sys.error(s"File not found $path")
      }
      val json = Json.parse(stream)
      verify(json)
    }

    verifyAgainstJson()
    verifyAgainstFile()
  }
}
