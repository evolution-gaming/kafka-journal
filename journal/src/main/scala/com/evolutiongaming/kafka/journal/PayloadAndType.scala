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
  * It usually stores [[Events]] instance in a serialized form.
  *
  * The usual practice is that [[PayloadAndType#payload]] will be used as a value for a Kafka record, and
  * [[PayloadAndType#payloadType]] will get into a header.
  *
  * @param payload
  *   Used to store actual payload, i.e. one or several journal events. The contents are usually a [[PayloadJson]]
  *   instance serialized into a JSON form.
  * @param payloadType
  *   Used to determine how the contents of `payload` should be treated, i.e. if it should be parsed as JSON.
  */
final case class PayloadAndType(payload: ByteVector, payloadType: PayloadType.BinaryOrJson)

object PayloadAndType {

  /** Single journal event with payload serialized into JSON or String form.
    *
    * It usually corresponds to a single [[Event]] instance with payload serialized to JSON form.
    *
    * @param seqNr
    *   Event sequence number in a journal.
    * @param tags
    *   Event tags as described in [[akka.persistence.journal.Tagged]].
    * @param payload
    *   Serialized payload in JSON or String form.
    * @param payloadType
    *   Type indicating if JSON should be parsed from a payload.
    *
    * @tparam A
    *   Type of a JSON library used. I.e. it could be `JsValue` for Play JSON or `Json` for Circe.
    */
  final case class EventJson[A](
    seqNr: SeqNr,
    tags: Tags,
    payloadType: Option[PayloadType.TextOrJson] = None,
    payload: Option[A] = None,
  )

  object EventJson {

    implicit val formatEventJson: OFormat[EventJson[JsValue]] = Json.format

    implicit val writesNelEventJson: Writes[Nel[EventJson[JsValue]]] = nelWrites

    implicit val readsNelEventJson: Reads[Nel[EventJson[JsValue]]] = nelReads
  }

  /** Payload of a single event serialized into JSON or String form.
    *
    * It represents two fields from [[EventJson]] class, i.e. [[EventJson#payload]] and [[EventJson#payloadType]].
    *
    * The reason this class was created is to make the logic constructing [[EventJson]] in
    * [[conversions.KafkaWrite#writeJson]] and [[conversions.KafkaRead#readJson]] generic by having ability to convert
    * from an actual payload to [[EventJsonPayloadAndType]] and back.
    *
    * To give an example, at the time of writing the following conversions were available:
    * ```
    * Payload <-> EventJsonPayloadAndType[JsValue] (in kafka-journal module)
    * Json    <-> EventJsonPayloadAndType[Json]    (in kafka-journal-circe module)
    * ```
    *
    * It might be possible to express the same logic without using the class, so in future it might be removed as an
    * overall simplification.
    *
    * It usually corresponds to a single `Payload` instance if Play JSON is used, or `Json` instance if Circe is used
    * instead.
    *
    * @param payload
    *   Serialized payload in JSON or String form.
    * @param payloadType
    *   Type indicating if JSON should be parsed from a payload.
    *
    * @tparam A
    *   Type of a JSON library used. I.e. it could be `JsValue` for Play JSON or `Json` for Circe.
    */
  final case class EventJsonPayloadAndType[A](payload: A, payloadType: PayloadType.TextOrJson)

  /** Multiple journal events with payloads serialized into JSON or String form.
    *
    * The class is meant to be serialized into JSON and stored into [[PayloadAndType#payload]] field.
    *
    * It usually corresonds to a single [[Events]] instance with all events serialized into JSON.
    *
    * @param events
    *   List of one or multiple events.
    * @param metadata
    *   Metadata shared by all events in the list.
    *
    * @tparam A
    *   Type of a JSON library used. I.e. it could be `JsValue` for Play JSON or `Json` for Circe.
    */
  final case class PayloadJson[A](events: Nel[EventJson[A]], metadata: Option[PayloadMetadata])

  object PayloadJson {

    implicit val formatPayloadJson: OFormat[PayloadJson[JsValue]] = Json.format

    implicit def codecPayloadJson(implicit jsonCodec: JsonCodec[Try]): Codec[PayloadJson[JsValue]] =
      formatCodec // TODO not used

    implicit def toBytesPayloadJson[F[_]: JsonCodec.Encode]: ToBytes[F, PayloadJson[JsValue]] =
      ToBytes.fromWrites

    implicit def fromBytesPayloadJson[F[_]: Monad: FromJsResult: JsonCodec.Decode]: FromBytes[F, PayloadJson[JsValue]] =
      FromBytes.fromReads
  }
}
