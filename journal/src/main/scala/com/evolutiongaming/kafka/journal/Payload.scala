package com.evolutiongaming.kafka.journal

import com.evolutiongaming.kafka.journal.util.PlayJsonHelper._
import com.evolutiongaming.kafka.journal.util.ScodecHelper._
import com.evolutiongaming.scassandra.{DecodeByName, EncodeByName}
import play.api.libs.json._
import play.api.libs.functional.syntax._
import scodec.Codec
import scodec.bits.ByteVector
import scodec.codecs.{bytes, utf8}

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

    def payloadType = PayloadType.Binary

    def size: Long = value.length
  }

  object Binary {

    val empty: Binary = Binary(ByteVector.empty)


    implicit val codecBinary: Codec[Binary] = bytes.as[Binary]

    implicit val encodeByNameBinary: EncodeByName[Binary] = EncodeByName[Array[Byte]].contramap(_.value.toArray)

    implicit val decodeByNameBinary: DecodeByName[Binary] = DecodeByName[Array[Byte]].map(a => Binary(ByteVector.view(a)))
  }


  final case class Text(value: String) extends Payload {
    def payloadType = PayloadType.Text
  }

  object Text {

    implicit val codecText: Codec[Text] = utf8.as[Payload.Text]
    

    implicit val encodeByNameText: EncodeByName[Text] = EncodeByName[String].contramap(_.value)

    implicit val decodeByNameText: DecodeByName[Text] = DecodeByName[String].map(Text(_))


//    implicit val ToBytesText: ToBytes[Text] = ToBytes[String].imap(_.value)

//    implicit val FromBytesText: FromBytes[Text] = FromBytes[String].map(Text(_))
  }


  final case class Json(value: JsValue) extends Payload {
    def payloadType = PayloadType.Json
  }

  object Json {

    implicit val formatJson: Format[Json] = Format.of[JsValue].inmap(Json(_), _.value)

    val codecJson: Codec[Json] = formatCodec


//    implicit val ToBytesJson: ToBytes[Json] = ToBytes.fromWrites

//    implicit val FromBytesJson: FromBytes[Json] = FromBytes.fromReads


    implicit val encodeByNameJson: EncodeByName[Json] = encodeByNameFromWrites

    implicit val decodeByNameJson: DecodeByName[Json] = decodeByNameFromReads


    def apply[A](a: A)(implicit writes: Writes[A]): Json = Json(writes.writes(a))
  }
}