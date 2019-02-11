package com.evolutiongaming.kafka.journal

import com.evolutiongaming.kafka.journal.PlayJsonHelper._
import com.evolutiongaming.scassandra.{DecodeByName, DecodeRow, EncodeByName, EncodeRow}
import play.api.libs.json.{JsValue, Json, OFormat}

final case class Metadata(data: Option[JsValue])

object Metadata {

  val Empty: Metadata = Metadata(data = None)
  

  implicit val FormatImpl: OFormat[Metadata] = Json.format[Metadata]


  implicit val EncodeByNameImpl: EncodeByName[Metadata] = EncodeByName[JsValue].imap(Json.toJson(_))

  implicit val DecodeByNameImpl: DecodeByName[Metadata] = DecodeByName[JsValue].map(_.as[Metadata])


  implicit val EncodeRowImpl: EncodeRow[Metadata] = EncodeRow("metadata")

  implicit val DecodeRowImpl: DecodeRow[Metadata] = DecodeRow("metadata")
}