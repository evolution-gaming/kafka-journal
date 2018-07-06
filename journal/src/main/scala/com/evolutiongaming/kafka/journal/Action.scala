package com.evolutiongaming.kafka.journal

import com.evolutiongaming.kafka.journal.Alias.SeqNr
import play.api.libs.json._

sealed trait Action

object Action {

  // TODO move out to separate object
  implicit val JsonFormat: OFormat[Action] = {

    implicit val SeqRangeFormat = Json.format[SeqRange]

    val AppendFormat = Json.format[Append]
    val TruncateFormat = Json.format[Truncate]
    val ReadFormat = Json.format[Mark]

    new OFormat[Action] {

      def reads(json: JsValue): JsResult[Action] = {
        def read[T](name: String, reads: Reads[T]) = {
          (json \ name).validate(reads)
        }

        read("append", AppendFormat) orElse read("mark", ReadFormat) orElse read("truncate", TruncateFormat)
      }

      def writes(action: Action): JsObject = {

        def write[T](name: String, value: T, writes: Writes[T]) = {
          val json = writes.writes(value)
          Json.obj(name -> json)
        }

        action match {
          case action: Append   => write("append", action, AppendFormat)
          case action: Mark     => write("mark", action, ReadFormat)
          case action: Truncate => write("truncate", action, TruncateFormat)
        }
      }
    }
  }


  sealed trait AppendOrTruncate extends Action
  case class Append(range: SeqRange) extends AppendOrTruncate
  case class Truncate(to: SeqNr) extends AppendOrTruncate
  case class Mark(id: String) extends Action
}