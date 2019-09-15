package com.evolutiongaming.kafka.journal

import com.evolutiongaming.kafka.journal.util.PlayJsonHelper._
import com.evolutiongaming.scassandra.{DecodeByName, EncodeByName}
import play.api.libs.json._

sealed abstract class PayloadType extends Product {
  def ext: String
  def name: String
}

object PayloadType {

  val values: Set[PayloadType] = Set(Binary, Text, Json)

  private val byName = values.map(value => (value.name, value)).toMap


  implicit val encodeByNamePayloadType: EncodeByName[PayloadType] = EncodeByName[String].imap { _.name }

  implicit val decodeByNamePayloadType: DecodeByName[PayloadType] = DecodeByName[String].map { name =>
    apply(name) getOrElse Binary
  }


  implicit val writesPayloadType: Writes[PayloadType] = Writes.of[String].contramap(_.name)

  implicit val readsPayloadType: Reads[PayloadType] = Reads.of[String].mapResult { a =>
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
    implicit val readsBinaryOrJson: Reads[BinaryOrJson] = Reads.of[String].mapResult { a =>
      apply(a) match {
        case Some(a: BinaryOrJson) => JsSuccess(a)
        case _                     => JsError(s"No PayloadType.BinaryOrJson found by $a")
      }
    }
  }


  sealed trait TextOrJson extends PayloadType

  object TextOrJson {
    implicit val readsTextOrJson: Reads[TextOrJson] = Reads.of[String].mapResult { a =>
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