package com.evolutiongaming.kafka.journal

import com.evolutiongaming.kafka.journal.PlayJsonHelper._
import com.evolutiongaming.scassandra.{DecodeByName, DecodeRow, EncodeByName, EncodeRow}
import play.api.libs.json.{JsValue, Json, OFormat}

final case class Metadata(id: Option[String]/*TODO remove this*/, data: Option[JsValue])

object Metadata {

  val Empty: Metadata = Metadata(id = None, data = None)
  

  implicit val FormatImpl: OFormat[Metadata] = Json.format[Metadata]


  implicit val EncodeByNameImpl: EncodeByName[Metadata] = EncodeByName[JsValue].imap { a => Json.toJson(a) }

  implicit val DecodeByNameImpl: DecodeByName[Metadata] = DecodeByName[JsValue].map(_.as[Metadata])


  implicit val EncodeRowImpl: EncodeRow[Metadata] = EncodeRow("metadata")

  implicit val DecodeRowImpl: DecodeRow[Metadata] = DecodeRow("metadata")
  

  def apply(id: String, data: Option[JsValue]): Metadata = Metadata(id = Some(id), data = data)

  def apply(seqNr: SeqNr): Metadata = Metadata(id = Some(seqNr.value.toString), None)
}