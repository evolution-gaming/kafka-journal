package com.evolutiongaming.kafka.journal

import com.evolutiongaming.kafka.journal.util.PlayJsonHelper._
import com.evolutiongaming.scassandra.{DecodeByName, DecodeRow, EncodeByName, EncodeRow}
import play.api.libs.json.{JsValue, Json, OFormat}

final case class RecordMetadata(data: Option[JsValue])

object RecordMetadata {

  val empty: RecordMetadata = RecordMetadata(data = None)


  implicit val formatRecordMetadata: OFormat[RecordMetadata] = Json.format


  implicit val encodeByNameRecordMetadata: EncodeByName[RecordMetadata] = encodeByNameFromWrites

  implicit val decodeByNameRecordMetadata: DecodeByName[RecordMetadata] = decodeByNameFromReads


  implicit val encodeRowRecordMetadata: EncodeRow[RecordMetadata] = EncodeRow("metadata")

  implicit val decodeRowRecordMetadata: DecodeRow[RecordMetadata] = DecodeRow("metadata")
}