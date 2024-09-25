package com.evolutiongaming.kafka.journal

import com.evolutiongaming.kafka.journal.util.ScodecHelper.*
import play.api.libs.functional.syntax.*
import play.api.libs.json.*
import scodec.bits.{BitVector, ByteVector}
import scodec.codecs.{bytes, utf8}
import scodec.{Attempt, Codec, DecodeResult, SizeBound}

import scala.util.Try

sealed abstract class Payload extends Product {

  def payloadType: PayloadType
}

object Payload {

  def apply(value: String): Payload = text(value)

  def apply(value: ByteVector): Payload = binary(value)

  def apply(value: JsValue): Payload = json(value)

  def text(value: String): Payload = Text(value)

  def binary(value: ByteVector): Payload = Binary(value)

  def json[A](value: A)(implicit writes: Writes[A]): Payload = Json(value)

  final case class Binary(value: ByteVector) extends Payload {

    def payloadType: PayloadType.Binary.type = PayloadType.Binary

    def size: Long = value.length
  }

  object Binary {

    val empty: Binary = Binary(ByteVector.empty)

    implicit val codecBinary: Codec[Binary] = new Codec[Binary] {
      override def decode(bits: BitVector): Attempt[DecodeResult[Binary]] = bytes.decode(bits).map(_.map(Binary(_)))

      override def encode(value: Binary): Attempt[BitVector] = bytes.encode(value.value)

      override def sizeBound: SizeBound = bytes.sizeBound
    }

  }

  sealed abstract class TextOrJson extends Payload {
    def payloadType: PayloadType.TextOrJson
  }

  final case class Text(value: String) extends TextOrJson {
    def payloadType: PayloadType.Text.type = PayloadType.Text
  }

  object Text {

    implicit val codecText: Codec[Text] = new Codec[Text] {
      override def decode(bits: BitVector): Attempt[DecodeResult[Text]] = utf8.decode(bits).map(_.map(Text(_)))

      override def encode(value: Text): Attempt[BitVector] = utf8.encode(value.value)

      override def sizeBound: SizeBound = utf8.sizeBound
    }

  }

  final case class Json(value: JsValue) extends TextOrJson {
    def payloadType: PayloadType.Json.type = PayloadType.Json
  }

  object Json {

    implicit val formatJson: Format[Json] = Format.of[JsValue].inmap(Json(_), _.value)

    def codecJson(implicit jsonCodec: JsonCodec[Try]): Codec[Json] = formatCodec

    def apply[A](a: A)(implicit writes: Writes[A]): Json = Json(writes.writes(a))
  }
}
