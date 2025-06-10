package com.evolutiongaming.kafka.journal

import play.api.libs.json.*
import play.api.libs.json.Json.WithDefaultValues

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

  implicit class RecordMetadataOps(val self: RecordMetadata) extends AnyVal {

    def withExpireAfter(expireAfter: Option[ExpireAfter]): RecordMetadata = {
      self.copy(payload = self.payload.copy(expireAfter = expireAfter))
    }
  }
}
