package com.evolutiongaming.kafka.journal

import cats.Applicative
import play.api.libs.json._


sealed abstract class ActionHeader extends Product {
  
  def origin: Option[Origin]
}

object ActionHeader {

  val key: String = "journal.action"


  implicit val formatActionHeader: OFormat[ActionHeader] = {

    val appendFormat = {
      val format = Json.format[Append]
      val reads = format orElse new Reads[Append] {
        def reads(json: JsValue) = {
          for {
            range       <- (json \ "range").validate[SeqRange]
            origin      <- (json \ "origin").validateOpt[Origin]
            payloadType <- (json \ "payloadType").validate[PayloadType.BinaryOrJson]
            expireAfter <- (json \ "expireAfter").validateOpt[ExpireAfter]
            metadata    <- (json \ "metadata").validateOpt[RecordMetadata]
          } yield {
            Append(
              range = range,
              origin = origin,
              payloadType = payloadType,
              metadata = metadata getOrElse RecordMetadata.empty,
              expireAfter = expireAfter)
          }
        }
      }
      OFormat(reads, format)
    }
    val deleteFormat = Json.format[Delete]
    val purgeFormat = Json.format[Purge]
    val readFormat = Json.format[Mark]

    new OFormat[ActionHeader] {

      def reads(json: JsValue): JsResult[ActionHeader] = {
        def read[T](name: String, reads: Reads[T]) = {
          (json \ name).validate(reads)
        }

        // TODO expiry: make sure we can roll out new actions without client update
        read("append", appendFormat) orElse
          read("mark", readFormat) orElse
          read("delete", deleteFormat) orElse
          read("purge", purgeFormat)
      }

      def writes(header: ActionHeader): JsObject = {

        def write[T](name: String, value: T, writes: Writes[T]) = {
          val json = writes.writes(value)
          Json.obj(name -> json)
        }

        header match {
          case header: Append => write("append", header, appendFormat)
          case header: Mark   => write("mark", header, readFormat)
          case header: Delete => write("delete", header, deleteFormat)
          case header: Purge  => write("purge", header, purgeFormat)
        }
      }
    }
  }

  implicit def toBytesActionHeader[F[_] : Applicative]: ToBytes[F, ActionHeader] = ToBytes.fromWrites

  implicit def fromBytesActionHeader[F[_] : FromJsResult]: FromBytes[F, ActionHeader] = FromBytes.fromReads


  sealed abstract class AppendOrDelete extends ActionHeader


  final case class Append(
    range: SeqRange,
    origin: Option[Origin],
    payloadType: PayloadType.BinaryOrJson,
    expireAfter: Option[ExpireAfter], // TODO expiry: change order in other places, add to logging
    metadata: RecordMetadata, // TODO expiry: use HeaderMetadata
  ) extends AppendOrDelete


  final case class Delete(
    to: SeqNr,
    origin: Option[Origin]
  ) extends AppendOrDelete


  final case class Purge(
    origin: Option[Origin]
  ) extends AppendOrDelete


  final case class Mark(
    id: String,
    origin: Option[Origin]
  ) extends ActionHeader
}
