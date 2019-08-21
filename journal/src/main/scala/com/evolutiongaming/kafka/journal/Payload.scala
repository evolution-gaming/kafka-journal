package com.evolutiongaming.kafka.journal

import com.evolutiongaming.kafka.journal.PlayJsonHelper._
import com.evolutiongaming.scassandra.{DecodeByName, EncodeByName}
import play.api.libs.json._

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


    def apply[A](a: A)(implicit writes: Writes[A]): Json = Json(writes.writes(a))
  }
}


sealed abstract class PayloadType extends Product {
  def ext: String
  def name: String
}

object PayloadType {

  val Values: Set[PayloadType] = Set(Binary, Text, Json)

  private val byName = Values.map(value => (value.name, value)).toMap


  implicit val EncodeImp: EncodeByName[PayloadType] = EncodeByName[String].imap { _.name }

  implicit val DecodeImp: DecodeByName[PayloadType] = DecodeByName[String].map { name =>
    apply(name) getOrElse Binary
  }


  implicit val WritesPayloadType: Writes[PayloadType] = WritesOf[String].contramap(_.name)

  implicit val ReadsPayloadType: Reads[PayloadType] = ReadsOf[String].mapResult { a =>
    apply(a) match {
      case Some(a) => JsSuccess(a)
      case None    => JsError(s"No PayloadType found by $a")
    }
  }


  def apply(name: String): Option[PayloadType] = byName.get(name)


  def binary: PayloadType = Binary

  def text: PayloadType = Text

  def json: PayloadType = Json


  sealed abstract class BinaryOrJson extends PayloadType

  object BinaryOrJson {
    implicit val ReadsBinaryOrJson: Reads[BinaryOrJson] = ReadsOf[String].mapResult { a =>
      apply(a) match {
        case Some(a: BinaryOrJson) => JsSuccess(a)
        case _                     => JsError(s"No PayloadType.BinaryOrJson found by $a")
      }
    }
  }


  sealed trait TextOrJson extends PayloadType

  object TextOrJson {
    implicit val ReadsTextOrJson: Reads[TextOrJson] = ReadsOf[String].mapResult { a =>
      apply(a) match {
        case Some(a: TextOrJson) => JsSuccess(a)
        case _                   => JsError(s"No PayloadType.TextOrJson found by $a")
      }
    }
  }


  case object Binary extends BinaryOrJson {
    def name = "binary"
    def ext = "bin"
  }

  case object Text extends TextOrJson {
    def name = "text"
    def ext = "txt"
  }

  case object Json extends BinaryOrJson with TextOrJson {
    def name = "json"
    def ext = "json"
  }
}