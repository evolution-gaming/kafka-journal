package com.evolutiongaming.kafka.journal

import play.api.libs.json._

sealed trait ActionHeader { self =>

  def origin: Option[Origin]

  final def toBytes: Array[Byte] = {
    val json = Json.toJson(self)
    Json.toBytes(json)
  }
}

object ActionHeader {

  implicit val JsonFormat: OFormat[ActionHeader] = {

    implicit val SeqRangeFormat = Json.format[SeqRange]

    val AppendFormat = Json.format[Append]
    val DeleteFormat = Json.format[Delete]
    val ReadFormat = Json.format[Mark]

    new OFormat[ActionHeader] {

      def reads(json: JsValue): JsResult[ActionHeader] = {
        def read[T](name: String, reads: Reads[T]) = {
          (json \ name).validate(reads)
        }

        read("append", AppendFormat) orElse
          read("mark", ReadFormat) orElse
          read("delete", DeleteFormat)
      }

      def writes(header: ActionHeader): JsObject = {

        def write[T](name: String, value: T, writes: Writes[T]) = {
          val json = writes.writes(value)
          Json.obj(name -> json)
        }

        header match {
          case header: Append => write("append", header, AppendFormat)
          case header: Mark   => write("mark", header, ReadFormat)
          case header: Delete => write("delete", header, DeleteFormat)
        }
      }
    }
  }


  def apply(action: Action): ActionHeader = {
    action match {
      case action: Action.Append => Append(action.range, action.origin)
      case action: Action.Delete => Delete(action.to, action.origin)
      case action: Action.Mark   => Mark(action.id, action.origin)
    }
  }


  def apply(bytes: Array[Byte]): ActionHeader = {
    val json = Json.parse(bytes)
    json.as[ActionHeader]
  }


  final case class Append(range: SeqRange, origin: Option[Origin]) extends ActionHeader

  final case class Delete(to: SeqNr, origin: Option[Origin]) extends ActionHeader

  final case class Mark(id: String, origin: Option[Origin]) extends ActionHeader
}
