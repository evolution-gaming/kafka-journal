package com.evolutiongaming.kafka.journal

import play.api.libs.json._

sealed abstract class ActionHeader extends Product {
  def origin: Option[Origin]
}

object ActionHeader {

  implicit val FormatImpl: OFormat[ActionHeader] = {

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

  implicit val ToBytesImpl: ToBytes[ActionHeader] = ToBytes[JsValue].imap(Json.toJson(_))

  implicit val FromBytesImpl: FromBytes[ActionHeader] = FromBytes[JsValue].map(_.as[ActionHeader])


  sealed abstract class AppendOrDelete extends ActionHeader


  final case class Append(
    range: SeqRange,
    origin: Option[Origin],
    payloadType: PayloadType.BinaryOrJson,
    metadata: Option[JsValue]
  ) extends AppendOrDelete


  final case class Delete(
    to: SeqNr,
    origin: Option[Origin]
  ) extends AppendOrDelete


  final case class Mark(
    id: String,
    origin: Option[Origin]
  ) extends ActionHeader
}
