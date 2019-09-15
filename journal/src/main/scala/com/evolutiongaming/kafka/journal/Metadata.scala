package com.evolutiongaming.kafka.journal

import com.evolutiongaming.kafka.journal.util.PlayJsonHelper._
import com.evolutiongaming.scassandra.{DecodeByName, DecodeRow, EncodeByName, EncodeRow}
import play.api.libs.json.{JsValue, Json, OFormat}

final case class Metadata(data: Option[JsValue])

object Metadata {

  val empty: Metadata = Metadata(data = None)
  

  implicit val formatMetadata: OFormat[Metadata] = Json.format


  implicit val encodeByNameMetadata: EncodeByName[Metadata] = encodeByNameFromWrites

  implicit val decodeByNameMetadata: DecodeByName[Metadata] = decodeByNameFromReads


  implicit val encodeRowMetadata: EncodeRow[Metadata] = EncodeRow("metadata")

  implicit val decodeRowMetadata: DecodeRow[Metadata] = DecodeRow("metadata")
}