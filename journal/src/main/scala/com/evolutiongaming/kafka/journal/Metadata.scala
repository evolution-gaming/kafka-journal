package com.evolutiongaming.kafka.journal

import com.evolutiongaming.kafka.journal.util.PlayJsonHelper._
import com.evolutiongaming.scassandra.{DecodeByName, DecodeRow, EncodeByName, EncodeRow}
import play.api.libs.json.{JsValue, Json, OFormat}

final case class Metadata(data: Option[JsValue])

object Metadata {

  val Empty: Metadata = Metadata(data = None)
  

  implicit val FormatMetadata: OFormat[Metadata] = Json.format


  implicit val EncodeByNameMetadata: EncodeByName[Metadata] = encodeByNameFromWrites

  implicit val DecodeByNameMetadata: DecodeByName[Metadata] = decodeByNameFromReads


  implicit val EncodeRowMetadata: EncodeRow[Metadata] = EncodeRow("metadata")

  implicit val DecodeRowMetadata: DecodeRow[Metadata] = DecodeRow("metadata")
}