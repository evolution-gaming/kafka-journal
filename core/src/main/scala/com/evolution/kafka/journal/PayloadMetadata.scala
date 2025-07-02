package com.evolution.kafka.journal

import cats.Monad
import cats.implicits.none
import com.evolution.kafka.journal.util.ScodecHelper.formatCodec
import play.api.libs.json.{JsValue, Json, OFormat}
import scodec.*

import scala.util.Try

final case class PayloadMetadata(expireAfter: Option[ExpireAfter] = none, data: Option[JsValue] = none)

object PayloadMetadata {

  val empty: PayloadMetadata = PayloadMetadata(none, none)

  implicit val formatPayloadMetadata: OFormat[PayloadMetadata] = Json.format

  implicit def metadataCodec(
    implicit
    jsonCodec: JsonCodec[Try],
  ): Codec[PayloadMetadata] =
    formatCodec[PayloadMetadata]

  implicit def toBytesPayloadMetadata[F[_]: JsonCodec.Encode]: ToBytes[F, PayloadMetadata] =
    ToBytes.fromWrites

  implicit def fromBytesPayloadMetadata[F[_]: Monad: FromJsResult: JsonCodec.Decode]: FromBytes[F, PayloadMetadata] =
    FromBytes.fromReads
}
