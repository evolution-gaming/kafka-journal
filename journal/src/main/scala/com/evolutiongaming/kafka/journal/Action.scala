package com.evolutiongaming.kafka.journal

import com.evolutiongaming.kafka.journal.Alias.SeqNr
import play.api.libs.json._

sealed trait Action

object Action {

  // TODO move out to separate object
  implicit val JsonFormat: OFormat[Action] = {

    implicit val SeqRangeFormat = Json.format[SeqRange]

    val AppendFormat = Json.format[Append]
    val DeleteFormat = Json.format[Delete]
    val ReadFormat = Json.format[Mark]

    new OFormat[Action] {

      def reads(json: JsValue): JsResult[Action] = {
        def read[T](name: String, reads: Reads[T]) = {
          (json \ name).validate(reads)
        }

        read("append", AppendFormat) orElse read("mark", ReadFormat) orElse read("delete", DeleteFormat)
      }

      def writes(action: Action): JsObject = {

        def write[T](name: String, value: T, writes: Writes[T]) = {
          val json = writes.writes(value)
          Json.obj(name -> json)
        }

        action match {
          case action: Append => write("append", action, AppendFormat)
          case action: Mark   => write("mark", action, ReadFormat)
          case action: Delete => write("delete", action, DeleteFormat)
        }
      }
    }
  }


  sealed trait AppendOrDelete extends Action
  // TODO make all case classes as Final
  case class Append(range: SeqRange) extends AppendOrDelete
  case class Delete(to: SeqNr) extends AppendOrDelete
  case class Mark(id: String) extends Action
}