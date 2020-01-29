package com.evolutiongaming.kafka.journal

import cats.{Applicative, Monad}
import cats.implicits.none
import play.api.libs.json.{JsValue, Json, OFormat}

final case class PayloadMetadata(
  expireAfter: Option[ExpireAfter] = none,
  data: Option[JsValue] = none)

object PayloadMetadata {

  val empty: PayloadMetadata = PayloadMetadata(none, none)

  implicit val formatPayloadMetadata: OFormat[PayloadMetadata] = Json.format

  implicit def toBytesPayloadMetadata[F[_] : Applicative : JsValueCodec.Encode]: ToBytes[F, PayloadMetadata] = ToBytes.fromWrites

  implicit def fromBytesPayloadMetadata[F[_] : Monad : FromJsResult : JsValueCodec.Decode]: FromBytes[F, PayloadMetadata] = FromBytes.fromReads
}
