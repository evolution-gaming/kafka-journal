package com.evolutiongaming.kafka.journal

import com.evolutiongaming.kafka.journal.util.ScodecHelper._
import play.api.libs.json._
import play.api.libs.functional.syntax._
import scodec.Codec
import scodec.*
import scodec.bits.ByteVector
import scodec.codecs.{bytes, utf8}

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

    implicit val codecBinary: Codec[Binary] = bytes.as[Binary]

  }

  sealed abstract class TextOrJson extends Payload {
    def payloadType: PayloadType.TextOrJson
  }

  final case class Text(value: String) extends TextOrJson {
    def payloadType: PayloadType.Text.type = PayloadType.Text
  }

  object Text {

    implicit val codecText: Codec[Text] = utf8.as[Text]

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