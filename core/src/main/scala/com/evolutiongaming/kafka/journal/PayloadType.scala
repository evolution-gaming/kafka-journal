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

  implicit val encodeByNamePayloadType: EncodeByName[PayloadType] = EncodeByName[String].contramap { _.name }

  implicit val decodeByNamePayloadType: DecodeByName[PayloadType] = DecodeByName[String].map { name =>
    apply(name) getOrElse Binary
  }

  implicit val formatPayloadType: Format[PayloadType] = {
    val writes = Writes
      .of[String]
      .contramap { (a: PayloadType) => a.name }

    val reads = Reads
      .of[String]
      .mapResult { a =>
        apply(a) match {
          case Some(a) => JsSuccess(a)
          case None    => JsError(s"No PayloadType found by $a")
        }
      }
    Format(reads, writes)
  }

  def apply(name: String): Option[PayloadType] = byName.get(name)

  def binary: PayloadType = Binary

  def text: PayloadType = Text

  def json: PayloadType = Json

  sealed abstract class BinaryOrJson extends PayloadType

  object BinaryOrJson {

    implicit val formatBinaryOrJson: Format[BinaryOrJson] = {
      val reads = Reads
        .of[String]
        .mapResult { a =>
          apply(a) match {
            case Some(a: BinaryOrJson) => JsSuccess(a)
            case _                     => JsError(s"No PayloadType.BinaryOrJson found by $a")
          }
        }
      val writes = formatPayloadType.as[BinaryOrJson]
      Format(reads, writes)
    }
  }

  sealed trait TextOrJson extends PayloadType

  object TextOrJson {

    implicit val formatTextOrJson: Format[TextOrJson] = {
      val reads = Reads
        .of[String]
        .mapResult { a =>
          apply(a) match {
            case Some(a: TextOrJson) => JsSuccess(a)
            case _                   => JsError(s"No PayloadType.TextOrJson found by $a")
          }
        }
      val writes = formatPayloadType.as[TextOrJson]
      Format(reads, writes)
    }
  }

  case object Binary extends BinaryOrJson {
    def name = "binary"
    def ext  = "bin"
  }

  case object Text extends TextOrJson {
    def name = "text"
    def ext  = "txt"
  }

  case object Json extends BinaryOrJson with TextOrJson {
    def name = "json"
    def ext  = "json"
  }
}
