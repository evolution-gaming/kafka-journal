package com.evolutiongaming.kafka.journal

import cats.Applicative
import cats.implicits.none
import play.api.libs.json.{JsValue, Json, OFormat}

final case class PayloadMetadata(
  expireAfter: Option[ExpireAfter] = none,
  data: Option[JsValue] = none)

object PayloadMetadata {

  val empty: PayloadMetadata = PayloadMetadata(none, none)

  implicit val formatPayloadMetadata: OFormat[PayloadMetadata] = Json.format

  implicit def toBytesPayloadMetadata[F[_] : Applicative]: ToBytes[F, PayloadMetadata] = ToBytes.fromWrites

  implicit def fromBytesPayloadMetadata[F[_] : FromJsResult]: FromBytes[F, PayloadMetadata] = FromBytes.fromReads
}
