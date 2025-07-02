package com.evolution.kafka.journal.util

import cats.syntax.all.*
import com.evolution.kafka.journal.util.PlayJsonHelper.*
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import play.api.libs.json.*

import scala.concurrent.duration.*

class FiniteDurationFormatTest extends AnyFunSuite with Matchers {

  for {
    (duration, expected) <- List((1.millis, "1 millisecond"), (100.minutes, "100 minutes"), (30.days, "30 days"))
  } {
    test(s"$duration to/from JsValue") {
      val jsValue = Json.toJson(duration)
      jsValue shouldEqual JsString(expected)
      jsValue.validate[FiniteDuration] shouldEqual duration.pure[JsResult]
    }
  }

  for {
    (json, expected) <- List(
      (JsString("1 min"), 1.minute),
      (JsNumber(2), 2.millis),
      (JsString("30 h"), 30.hours),
      (JsString("1 day"), 1.day),
    )
  } {
    test(s"$json from Duration") {
      json.validate[FiniteDuration] shouldEqual expected.pure[JsResult]
    }
  }

  test("parse error") {
    val expected = JsError("cannot parse FiniteDuration from test: java.lang.NumberFormatException: format error test")
    JsString("test").validate[FiniteDuration] shouldEqual expected
  }
}
