package com.evolutiongaming.kafka.journal

import com.evolutiongaming.kafka.journal.PlayJsonHelper._
import com.evolutiongaming.scassandra.{DecodeByName, EncodeByName}
import play.api.libs.json._

sealed trait Payload {
  def payloadType: PayloadType
}

object Payload {

  def apply(value: String): Payload = Text(value)

  def apply(value: Bytes): Payload = Binary(value)

  def apply(value: JsValue): Payload = Json(value)


  final case class Binary(value: Bytes) extends Payload {

    def payloadType = PayloadType.Binary

    def size: Int = value.length

    override def toString = s"$productPrefix($size)"
  }

  object Binary {

    val Empty: Binary = Binary(Bytes.Empty)

    implicit val ToBytesImpl: ToBytes[Binary] = ToBytes[Bytes].imap(_.value)

    implicit val FromBytesImpl: FromBytes[Binary] = FromBytes[Bytes].map(Binary(_))


    implicit val EncodeImpl: EncodeByName[Binary] = EncodeByName[Bytes].imap(_.value)

    implicit val DecodeImpl: DecodeByName[Binary] = DecodeByName[Bytes].map(Binary(_))


    def apply[A](a: A)(implicit toBytes: ToBytes[A]): Binary = Binary(toBytes(a))
  }


  final case class Text(value: String) extends Payload {
    def payloadType = PayloadType.Text
  }

  object Text {

    implicit val ToBytesImpl: ToBytes[Text] = ToBytes[String].imap(_.value)

    implicit val FromBytesImpl: FromBytes[Text] = FromBytes[String].map(Text(_))
    

    implicit val EncodeImpl: EncodeByName[Text] = EncodeByName[String].imap(_.value)

    implicit val DecodeImpl: DecodeByName[Text] = DecodeByName[String].map(Text(_))
  }


  final case class Json(value: JsValue) extends Payload {
    def payloadType = PayloadType.Json
  }

  object Json {

    implicit val ToBytesImpl: ToBytes[Json] = ToBytes[JsValue].imap(_.value)

    implicit val FromBytesImpl: FromBytes[Json] = FromBytes[JsValue].map(Json(_))


    implicit val EncodeImpl: EncodeByName[Json] = EncodeByName[JsValue].imap(_.value)

    implicit val DecodeImpl: DecodeByName[Json] = DecodeByName[JsValue].map(Json(_))


    implicit val WritesImpl: Writes[Json] = WritesOf[JsValue].imap(_.value)

    implicit val ReadsImpl: Reads[Json] = ReadsOf[JsValue].mapResult(a => JsSuccess(Json(a)))


    def apply[A](a: A)(implicit writes: Writes[A]): Json = Json(writes.writes(a))
  }
}


sealed trait PayloadType {
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


  implicit val WritesImpl: Writes[PayloadType] = WritesOf[String].imap(_.name)

  implicit val ReadsImpl: Reads[PayloadType] = ReadsOf[String].mapResult { a =>
    apply(a) match {
      case Some(a) => JsSuccess(a)
      case None    => JsError(s"No PayloadType found by $a")
    }
  }


  def apply(name: String): Option[PayloadType] = byName.get(name)


  sealed trait BinaryOrJson extends PayloadType

  object BinaryOrJson {
    implicit val ReadsImpl: Reads[BinaryOrJson] = ReadsOf[String].mapResult { a =>
      apply(a) match {
        case Some(a: BinaryOrJson) => JsSuccess(a)
        case _                     => JsError(s"No PayloadType.BinaryOrJson found by $a")
      }
    }
  }


  sealed trait TextOrJson extends PayloadType

  object TextOrJson {
    implicit val ReadsImpl: Reads[TextOrJson] = ReadsOf[String].mapResult { a =>
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