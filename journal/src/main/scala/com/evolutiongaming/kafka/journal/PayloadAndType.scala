package com.evolutiongaming.kafka.journal


import cats.Monad
import cats.data.{NonEmptyList => Nel}
import com.evolutiongaming.kafka.journal.util.PlayJsonHelper._
import com.evolutiongaming.kafka.journal.util.ScodecHelper._
import play.api.libs.json._
import scodec.Codec
import scodec.bits.ByteVector

import scala.util.Try


/** Piece of data prepared for convenient storing into Kafka record.
  *
  * The usual practice is that [[PayloadAndType#payload]] will be used as a
  * value for a Kafka record, and [[PayloadAndType#payloadType]] will get into a
  * header.
  *
  * @param payload
  *   Used to store actual payload, i.e. one or several journal events.
  * @param payloadType
  *   Used to determine how the contents of `payload` should be treated, i.e. if
  *   it should be parsed as JSON.
  */
final case class PayloadAndType(
  payload: ByteVector,
  payloadType: PayloadType.BinaryOrJson)

object PayloadAndType {

  final case class EventJson[A](
    seqNr: SeqNr,
    tags: Tags,
    payloadType: Option[PayloadType.TextOrJson] = None,
    payload: Option[A] = None)

  object EventJson {

    implicit val formatEventJson: OFormat[EventJson[JsValue]] = Json.format


    implicit val writesNelEventJson: Writes[Nel[EventJson[JsValue]]] = nelWrites

    implicit val readsNelEventJson: Reads[Nel[EventJson[JsValue]]] = nelReads
  }


  /** Payload of a single event serialized into JSON or String form.
    *
    * @param payload
    *   Serialized payload in JSON or String form.
    * @param payloadType
    *   Type indicating if JSON should be parsed from a payload.
    *
    * @tparam A
    *   Type of a JSON library used. I.e. it could be `JsValue` for Play JSON or
    *   `Json` for Circe.
    */
  final case class EventJsonPayloadAndType[A](payload: A, payloadType: PayloadType.TextOrJson)


  final case class PayloadJson[A](events: Nel[EventJson[A]], metadata: Option[PayloadMetadata])

  object PayloadJson {

    implicit val formatPayloadJson: OFormat[PayloadJson[JsValue]] = Json.format


    implicit def codecPayloadJson(implicit jsonCodec: JsonCodec[Try]): Codec[PayloadJson[JsValue]] = formatCodec // TODO not used


    implicit def toBytesPayloadJson[F[_] : JsonCodec.Encode]: ToBytes[F, PayloadJson[JsValue]] =
      ToBytes.fromWrites

    implicit def fromBytesPayloadJson[F[_] : Monad : FromJsResult: JsonCodec.Decode]: FromBytes[F, PayloadJson[JsValue]] =
      FromBytes.fromReads
  }
}