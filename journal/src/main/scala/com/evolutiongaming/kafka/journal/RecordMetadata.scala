package com.evolutiongaming.kafka.journal

import com.evolutiongaming.kafka.journal.util.PlayJsonHelper._
import com.evolutiongaming.scassandra.{DecodeByName, DecodeRow, EncodeByName, EncodeRow}
import play.api.libs.json._

final case class RecordMetadata(
  header: HeaderMetadata = HeaderMetadata.empty,
  payload: PayloadMetadata = PayloadMetadata.empty)

object RecordMetadata {

  val empty: RecordMetadata = RecordMetadata()


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


  implicit class RecordMetadataOps(val self: RecordMetadata) extends AnyVal {

    def withExpireAfter(expireAfter: Option[ExpireAfter]): RecordMetadata = {
      self.copy(payload = self.payload.copy(expireAfter = expireAfter))
    }
  }
}