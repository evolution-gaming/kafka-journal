package com.evolutiongaming.kafka.journal

import com.evolutiongaming.kafka.journal.Action.Header
import com.evolutiongaming.kafka.journal.Action.Header._
import play.api.libs.json._

object HeaderFormats {

  implicit val SeqNrFormat: Format[SeqNr] = new Format[SeqNr] {

    def reads(json: JsValue): JsResult[SeqNr] = for {
      value <- json.validate[Long]
      seqNr <- SeqNr.validate(value)(JsError(_), JsSuccess(_))
    } yield seqNr

    def writes(seqNr: SeqNr) = JsNumber(seqNr.value)
  }

  implicit val JsonFormat: OFormat[Header] = {

    implicit val SeqRangeFormat = Json.format[SeqRange]

    val AppendFormat = Json.format[Append]
    val DeleteFormat = Json.format[Delete]
    val ReadFormat = Json.format[Mark]

    new OFormat[Header] {

      def reads(json: JsValue): JsResult[Header] = {
        def read[T](name: String, reads: Reads[T]) = {
          (json \ name).validate(reads)
        }

        read("append", AppendFormat) orElse read("mark", ReadFormat) orElse read("delete", DeleteFormat)
      }

      def writes(header: Header): JsObject = {

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
}
