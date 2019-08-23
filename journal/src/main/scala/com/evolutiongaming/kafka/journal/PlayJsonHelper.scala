package com.evolutiongaming.kafka.journal

import cats.data.{NonEmptyList => Nel}
import com.evolutiongaming.scassandra.{DecodeByName, EncodeByName}
import play.api.libs.json._


object PlayJsonHelper {

  implicit val JsonEncodeByName: EncodeByName[JsValue] = encodeByNameFromWrites

  implicit val JsonDecodeByName: DecodeByName[JsValue] = decodeByNameFromReads

  implicit val JsonOptEncodeByName: EncodeByName[Option[JsValue]] = EncodeByName.opt

  implicit val JsonOptDecodeByName: DecodeByName[Option[JsValue]] = DecodeByName.opt


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


  def encodeByNameFromWrites[A](implicit writes: Writes[A]): EncodeByName[A] = EncodeByName[String].imap { a =>
    val jsValue = writes.writes(a)
    Json.stringify(jsValue)
  }

  
  def decodeByNameFromReads[A](implicit reads: Reads[A]): DecodeByName[A] = DecodeByName[String].map { a =>
    val jsValue = Json.parse(a)
    jsValue.as(reads)
  }


  implicit class FormatOps[A](val self: Format[A]) extends AnyVal {

    def bimap[B](to: A => B, from: B => A): Format[B] = {
      val reads = self.map(to)
      val writes = self.contramap(from)
      Format(reads, writes)
    }
  }
}
