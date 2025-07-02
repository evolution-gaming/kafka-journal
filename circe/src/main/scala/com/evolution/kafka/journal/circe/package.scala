package com.evolution.kafka.journal

import cats.Eval
import cats.syntax.all.*
import io.circe.Json
import play.api.libs.json.*

package object circe {

  def convertPlayToCirce(jsValue: JsValue): Json = {
    def convert(jsValue: JsValue): Eval[Json] = jsValue match {
      case JsNull => Eval.now(Json.Null)
      case JsString(a) => Eval.now(Json.fromString(a))
      case JsNumber(a) => Eval.now(Json.fromBigDecimal(a))
      case JsTrue => Eval.now(Json.True)
      case JsFalse => Eval.now(Json.False)
      case JsArray(values) =>
        values
          .toList
          .traverse { value => Eval.defer(convert(value)) }
          .map(Json.arr(_*))
      case JsObject(fields) =>
        fields
          .toList
          .traverse { case (field, value) => Eval.defer(convert(value)).map(field -> _) }
          .map(Json.fromFields)
    }
    convert(jsValue).value
  }

  def convertCirceToPlay(json: Json): Either[String, JsValue] = {

    def convert(json: Json): Eval[Either[String, JsValue]] = json.fold(
      Eval.now(JsNull.asRight[String]),
      bool => Eval.now(JsBoolean(bool).asRight[String]),
      num => Eval.now(num.toBigDecimal.map(JsNumber.apply).toRight(s"Failed to convert JsonNumber $num to JsNumber")),
      str => Eval.now(JsString(str).asRight[String]),
      arr =>
        arr
          .toList
          .traverse { value =>
            Eval.defer(convert(value))
          }
          .map(_.sequence.map(JsArray(_))),
      obj =>
        obj
          .toList
          .traverse {
            case (field, value) =>
              Eval.defer(convert(value)).map(_.map(field -> _))
          }
          .map(_.sequence.map(JsObject(_))),
    )

    convert(json).value
  }

}
