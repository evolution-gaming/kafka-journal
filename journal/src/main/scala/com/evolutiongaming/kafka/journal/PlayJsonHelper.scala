package com.evolutiongaming.kafka.journal

import cats.data.{NonEmptyList => Nel}
import com.evolutiongaming.scassandra.{DecodeByName, EncodeByName}
import play.api.libs.json._

object PlayJsonHelper {

  implicit val JsonEncode: EncodeByName[JsValue] = EncodeByName[String].imap(Json.stringify)

  implicit val JsonDecode: DecodeByName[JsValue] = DecodeByName[String].map(Json.parse)

  implicit val JsonOptEncode: EncodeByName[Option[JsValue]] = EncodeByName.opt[JsValue]

  implicit val JsonOptDecode: DecodeByName[Option[JsValue]] = DecodeByName.opt[JsValue]


  object ReadsOf {
    def apply[A](implicit F: Reads[A]): Reads[A] = F
  }


  object WritesOf {
    def apply[A](implicit F: Writes[A]): Writes[A] = F
  }


  object FormatOf {
    def apply[A](implicit F: Format[A]): Format[A] = F
  }


  implicit def nelReads[T](implicit reads: Reads[List[T]]): Reads[Nel[T]] = {
    reads.mapResult {
      case Nil          => JsError("list is empty")
      case head :: tail => JsSuccess(Nel(head, tail))
    }
  }


  implicit def nelWrites[A](implicit writes: Writes[List[A]]): Writes[Nel[A]] = {
    writes.contramap(_.toList)
  }


  implicit class ReadsOps[A](val self: Reads[A]) extends AnyVal {

    final def mapResult[B](f: A => JsResult[B]): Reads[B] = (a: JsValue) => self.reads(a).flatMap(f)
  }


  implicit class FormatOps[A](val self: Format[A]) extends AnyVal {

    def bimap[B](to: B => A)(from: A => JsResult[B]): Format[B] = {
      val reads = self.mapResult(from)
      val writes = self.contramap(to)
      Format(reads, writes)
    }
  }
}
