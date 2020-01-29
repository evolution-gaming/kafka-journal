package com.evolutiongaming.kafka.journal

import cats.{Applicative, Monad}
import cats.implicits._
import com.evolutiongaming.kafka.journal.util.PlayJsonHelper._
import play.api.libs.json._


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

          def metadata = {
            (json \ "metadata").validate[JsObject] match {
              case JsSuccess(a, _) => a.validate[HeaderMetadata]
              case _: JsError      => HeaderMetadata.empty.pure[JsResult]
            }
          }

          for {
            range       <- (json \ "range").validate[SeqRange]
            origin      <- (json \ "origin").validateOpt[Origin]
            payloadType <- (json \ "payloadType").validate[PayloadType.BinaryOrJson]
            metadata    <- metadata
          } yield {
            Append(range, origin, payloadType, metadata)
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


  implicit def toBytesActionHeader[F[_] : Applicative: JsValueCodec.Encode]: ToBytes[F, ActionHeader] = ToBytes.fromWrites

  implicit def fromBytesOptActionHeader[F[_] : Monad : FromJsResult : JsValueCodec.Decode]: FromBytes[F, Option[ActionHeader]] = FromBytes.fromReads


  sealed abstract class AppendOrDelete extends ActionHeader


  final case class Append(
    range: SeqRange,
    origin: Option[Origin],
    payloadType: PayloadType.BinaryOrJson,
    metadata: HeaderMetadata,
  ) extends AppendOrDelete


  final case class Delete(
    to: DeleteTo,
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
