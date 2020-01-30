package com.evolutiongaming.kafka.journal

import cats.{Applicative, Monad}
import cats.implicits.none
import play.api.libs.json.{JsValue, Json, OFormat}

final case class HeaderMetadata(data: Option[JsValue])

object HeaderMetadata {

  val empty: HeaderMetadata = HeaderMetadata(none)

  implicit val formatHeaderMetadata: OFormat[HeaderMetadata] = Json.format

  implicit def toBytesHeaderMetadata[F[_] : Applicative : JsonCodec.Encode]: ToBytes[F, HeaderMetadata] =
    ToBytes.fromWrites

  implicit def fromBytesHeaderMetadata[F[_] : Monad : FromJsResult : JsonCodec.Decode]: FromBytes[F, HeaderMetadata] =
    FromBytes.fromReads
}