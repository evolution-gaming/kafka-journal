package com.evolutiongaming.kafka.journal

import cats.Monad
import cats.syntax.all.*
import com.evolutiongaming.kafka.journal.util.PlayJsonHelper.*
import play.api.libs.json.*

sealed abstract class ActionHeader extends Product {

  def origin: Option[Origin]

  def version: Version
}

object ActionHeader {

  val key: String = "journal.action"

  implicit val formatOptActionHeader: OFormat[Option[ActionHeader]] = {

    val appendFormat = Json.using[Json.WithDefaultValues].format[Append]
    val deleteFormat = Json.using[Json.WithDefaultValues].format[Delete]
    val purgeFormat  = Json.using[Json.WithDefaultValues].format[Purge]
    val readFormat   = Json.using[Json.WithDefaultValues].format[Mark]

    new OFormat[Option[ActionHeader]] {

      def reads(json: JsValue): JsResult[Option[ActionHeader]] = {

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

  implicit val writesActionHeader: Writes[ActionHeader] = formatOptActionHeader.contramap { (a: ActionHeader) => a.some }

  implicit def toBytesActionHeader[F[_]: JsonCodec.Encode]: ToBytes[F, ActionHeader] = ToBytes.fromWrites

  implicit def fromBytesOptActionHeader[F[_]: Monad: FromJsResult: JsonCodec.Decode]: FromBytes[F, Option[ActionHeader]] =
    FromBytes.fromReads

  sealed abstract class AppendOrDelete extends ActionHeader

  final case class Append(
    range: SeqRange,
    origin: Option[Origin],
    version: Version = Version.obsolete,
    payloadType: PayloadType.BinaryOrJson,
    metadata: HeaderMetadata = HeaderMetadata.empty,
  ) extends AppendOrDelete

  final case class Delete(
    to: DeleteTo,
    origin: Option[Origin],
    version: Version = Version.obsolete,
  ) extends AppendOrDelete

  final case class Purge(
    origin: Option[Origin],
    version: Version = Version.obsolete,
  ) extends AppendOrDelete

  final case class Mark(
    id: String,
    origin: Option[Origin],
    version: Version = Version.obsolete,
  ) extends ActionHeader
}
