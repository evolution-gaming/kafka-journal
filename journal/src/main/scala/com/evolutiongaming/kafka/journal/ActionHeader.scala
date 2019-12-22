package com.evolutiongaming.kafka.journal

import cats.Applicative
import cats.implicits._
import play.api.libs.json._ // TODO expiry: replace with _
import com.evolutiongaming.kafka.journal.util.PlayJsonHelper.jsResultMonadError // TODO expiry:


sealed abstract class ActionHeader extends Product {
  
  def origin: Option[Origin]
}

object ActionHeader {

  val key: String = "journal.action"


  implicit val formatOptActionHeader: OFormat[Option[ActionHeader]] = {

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

    new OFormat[Option[ActionHeader]] {

      def reads(json: JsValue) = {

        def read[A](name: String, reads: Reads[A]) = {
          (json \ name)
            .validate[JsObject]
            .asOpt
            .map { _.validate(reads) }
        }

        read("append", appendFormat)
          .orElse(read("mark", readFormat))
          .orElse(read("delete", deleteFormat))
          .orElse(read("purge", purgeFormat))
          .sequence
      }

      def writes(header: Option[ActionHeader]): JsObject = {

        def write[A](name: String, value: A, writes: Writes[A]) = {
          val json = writes.writes(value)
          Json.obj((name, json))
        }

        header.fold {
          Json.obj()
        } {
          case header: Append => write("append", header, appendFormat)
          case header: Mark   => write("mark", header, readFormat)
          case header: Delete => write("delete", header, deleteFormat)
          case header: Purge  => write("purge", header, purgeFormat)
        }
      }
    }
  }

  implicit val writesActionHeader: Writes[ActionHeader] = formatOptActionHeader.contramap { a: ActionHeader => a.some }


  implicit def toBytesActionHeader[F[_] : Applicative]: ToBytes[F, ActionHeader] = ToBytes.fromWrites

  implicit def fromBytesOptActionHeader[F[_] : FromJsResult]: FromBytes[F, Option[ActionHeader]] = FromBytes.fromReads


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
