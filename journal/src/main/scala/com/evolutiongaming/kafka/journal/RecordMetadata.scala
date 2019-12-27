package com.evolutiongaming.kafka.journal

import com.evolutiongaming.kafka.journal.util.PlayJsonHelper._
import com.evolutiongaming.scassandra.{DecodeByName, DecodeRow, EncodeByName, EncodeRow}
import play.api.libs.json._

final case class RecordMetadata(
  header: HeaderMetadata,
  payload: PayloadMetadata)

object RecordMetadata {

  val empty: RecordMetadata = RecordMetadata(HeaderMetadata.empty, PayloadMetadata.empty)


  implicit val formatRecordMetadata: OFormat[RecordMetadata] = {

    val format = Json.format[RecordMetadata]

    // TODO expiry: temporary, remove after 01-02-2020
    val writes = new OWrites[RecordMetadata] {
      def writes(a: RecordMetadata) = {
        Json.toJsObject(a.header) ++ format.writes(a)
      }
    }

    val reads = format.orElse((json: JsValue) => {
      json
        .validate[HeaderMetadata]
        .map { a => RecordMetadata(a, PayloadMetadata.empty) }
    })

    OFormat(reads, writes)
  }


  implicit val encodeByNameRecordMetadata: EncodeByName[RecordMetadata] = encodeByNameFromWrites

  implicit val decodeByNameRecordMetadata: DecodeByName[RecordMetadata] = decodeByNameFromReads


  implicit val encodeRowRecordMetadata: EncodeRow[RecordMetadata] = EncodeRow("metadata")

  implicit val decodeRowRecordMetadata: DecodeRow[RecordMetadata] = DecodeRow("metadata")
}