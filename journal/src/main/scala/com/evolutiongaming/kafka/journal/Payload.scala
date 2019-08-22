package com.evolutiongaming.kafka.journal

import com.evolutiongaming.kafka.journal.PlayJsonHelper._
import com.evolutiongaming.scassandra.{DecodeByName, EncodeByName}
import play.api.libs.json._
import scodec.Codec
import scodec.bits.ByteVector
import scodec.codecs.{bytes, utf8}

sealed abstract class Payload extends Product {
  def payloadType: PayloadType
}

object Payload {

  def apply(value: String): Payload = text(value)

  def apply(value: Bytes): Payload = binary(value)

  def apply(value: JsValue): Payload = json(value)


  def text(value: String): Payload = Text(value)

  def binary[A](value: A)(implicit toBytes: ToBytes[A]): Payload = Binary(toBytes(value))

  def json[A](value: A)(implicit writes: Writes[A]): Payload = Json(value)


  // TODO replace with scodec
  final case class Binary(value: Bytes) extends Payload {

    def payloadType = PayloadType.Binary

    def size: Int = value.length

    override def toString = s"$productPrefix($size)"
  }

  object Binary {

    val Empty: Binary = Binary(Bytes.Empty)

    implicit val ToBytesBinary: ToBytes[Binary] = ToBytes[Bytes].imap(_.value)

    implicit val FromBytesBinary: FromBytes[Binary] = FromBytes[Bytes].map(Binary(_))


    implicit val EncodeByNameBinary: EncodeByName[Binary] = EncodeByName[Bytes].imap(_.value)

    implicit val DecodeByNameBinary: DecodeByName[Binary] = DecodeByName[Bytes].map(Binary(_))


    implicit val CodecBinary: Codec[Binary] = bytes.xmap[Binary](a => Binary(a.toArray), a => ByteVector(a.value))


    def apply[A](a: A)(implicit toBytes: ToBytes[A]): Binary = Binary(toBytes(a))
  }


  final case class Text(value: String) extends Payload {
    def payloadType = PayloadType.Text
  }

  object Text {

    implicit val ToBytesText: ToBytes[Text] = ToBytes[String].imap(_.value)

    implicit val FromBytesText: FromBytes[Text] = FromBytes[String].map(Text(_))
    

    implicit val EncodeByNameText: EncodeByName[Text] = EncodeByName[String].imap(_.value)

    implicit val DecodeByNameText: DecodeByName[Text] = DecodeByName[String].map(Text(_))


    implicit val CodecText: Codec[Text] = utf8.as[Payload.Text]
  }


  final case class Json(value: JsValue) extends Payload {
    def payloadType = PayloadType.Json
  }

  object Json {

    implicit val ToBytesJson: ToBytes[Json] = ToBytes[JsValue].imap(_.value)

    implicit val FromBytesJson: FromBytes[Json] = FromBytes[JsValue].map(Json(_))


    implicit val EncodeByNameJson: EncodeByName[Json] = EncodeByName[JsValue].imap(_.value)

    implicit val DecodeByNameJson: DecodeByName[Json] = DecodeByName[JsValue].map(Json(_))


    implicit val WritesJson: Writes[Json] = WritesOf[JsValue].contramap(_.value)

    implicit val ReadsJson: Reads[Json] = ReadsOf[JsValue].map(Json(_))


    implicit val CodecJson: Codec[Json] = JsValueCodec.as[Payload.Json]


    def apply[A](a: A)(implicit writes: Writes[A]): Json = Json(writes.writes(a))
  }
}