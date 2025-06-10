package com.evolutiongaming.kafka.journal

import com.evolutiongaming.kafka.journal.util.PlayJsonHelper.*
import com.evolutiongaming.scassandra.{DecodeByName, DecodeRow, EncodeByName, EncodeRow}
import play.api.libs.json.*
import play.api.libs.json.Json.WithDefaultValues

import scala.util.Try

final case class RecordMetadata(
  header: HeaderMetadata = HeaderMetadata.empty,
  payload: PayloadMetadata = PayloadMetadata.empty,
)

object RecordMetadata {

  val empty: RecordMetadata = RecordMetadata()

  implicit val formatRecordMetadata: OFormat[RecordMetadata] = {
    // adding WithDefaultValues, so on Scala 2 and Scala 3 the format behaves the same:
    // https://github.com/playframework/play-json/issues/1146
    val format = Json.using[WithDefaultValues].format[RecordMetadata]
    val reads = format
      .filter(_ != empty) // if empty is parsed, we might be dealing with an older format - HeaderMetadata
      .orElse { (json: JsValue) =>
        json
          .validate[HeaderMetadata]
          .map { a => RecordMetadata(a, PayloadMetadata.empty) }
      }
    OFormat(reads, format)
  }

  implicit def encodeByNameRecordMetadata(
    implicit
    encode: JsonCodec.Encode[Try],
  ): EncodeByName[RecordMetadata] =
    encodeByNameFromWrites

  implicit def decodeByNameRecordMetadata(
    implicit
    decode: JsonCodec.Decode[Try],
  ): DecodeByName[RecordMetadata] =
    decodeByNameFromReads

  implicit def encodeRowRecordMetadata(
    implicit
    encode: JsonCodec.Encode[Try],
  ): EncodeRow[RecordMetadata] =
    EncodeRow("metadata")

  implicit def decodeRowRecordMetadata(
    implicit
    decode: JsonCodec.Decode[Try],
  ): DecodeRow[RecordMetadata] =
    DecodeRow("metadata")

  implicit class RecordMetadataOps(val self: RecordMetadata) extends AnyVal {

    def withExpireAfter(expireAfter: Option[ExpireAfter]): RecordMetadata = {
      self.copy(payload = self.payload.copy(expireAfter = expireAfter))
    }
  }
}
