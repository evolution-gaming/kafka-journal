package com.evolutiongaming.kafka.journal.util

import java.nio.charset.StandardCharsets

import cats.implicits._
import cats.MonadError
import cats.arrow.FunctionK
import cats.data.{NonEmptyList => Nel}
import com.evolutiongaming.catshelper.ToTry
import com.evolutiongaming.catshelper.CatsHelper._
import com.evolutiongaming.kafka.journal.JsonCodec
import com.evolutiongaming.scassandra.{DecodeByName, EncodeByName}
import play.api.libs.json._
import scodec.bits.ByteVector

import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.util.Try


object PlayJsonHelper {

  implicit def jsValueEncodeByName(implicit encode: JsonCodec.Encode[Try]): EncodeByName[JsValue] =
    encodeByNameFromWrites

  implicit def jsValueDecodeByName(implicit decode: JsonCodec.Decode[Try]): DecodeByName[JsValue] =
    decodeByNameFromReads


  implicit def jsValueOptEncodeByName(implicit encode: JsonCodec.Encode[Try]): EncodeByName[Option[JsValue]] =
    EncodeByName.optEncodeByName

  implicit def jsValueOptDecodeByName(implicit decode: JsonCodec.Decode[Try]): DecodeByName[Option[JsValue]] =
    DecodeByName.optDecodeByName


  implicit val jsResultMonadError: MonadError[JsResult, JsError] = new MonadError[JsResult, JsError] {

    def raiseError[A](a: JsError) = a

    def handleErrorWith[A](fa: JsResult[A])(f: JsError => JsResult[A]) = {
      fa match {
        case fa: JsSuccess[A] => fa
        case fa: JsError      => f(fa)
      }
    }

    def pure[A](a: A) = JsSuccess(a)

    def flatMap[A, B](fa: JsResult[A])(f: A => JsResult[B]) = fa.flatMap(f)

    @tailrec
    def tailRecM[A, B](a: A)(f: A => JsResult[Either[A, B]]): JsResult[B] = {
      f(a) match {
        case b: JsSuccess[Either[A, B]] => b.value match {
          case Right(a) => JsSuccess(a)
          case Left(b)  => tailRecM(b)(f)
        }
        case b: JsError                 => b
      }
    }
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


  def encodeByNameFromWrites[A](implicit writes: Writes[A], encode: JsonCodec.Encode[Try]): EncodeByName[A] = {
    EncodeByName[String].contramap { a =>
      val jsValue = writes.writes(a)
      encode.toString(jsValue).get
    }
  }

  
  def decodeByNameFromReads[A](implicit reads: Reads[A], decode: JsonCodec.Decode[Try]): DecodeByName[A] = {
    DecodeByName[String].map { str =>
      val jsValue = decode.fromString(str).get
      // TODO not use `as`
      jsValue.as(reads)
    }
  }

  
  implicit val finiteDurationFormat: Format[FiniteDuration] = new Format[FiniteDuration] {

    def reads(json: JsValue) = {
      def fromString = for {
        str <- json.validate[String]
      } yield {
        Try { Duration(str).asInstanceOf[FiniteDuration] }
          .fold[JsResult[FiniteDuration]](
            a => JsError(s"cannot parse FiniteDuration from $str: $a"),
            a => JsSuccess(a))
      }

      def fromNumber = for {
        a <- json.validate[Long]
      } yield {
        a.millis
      }

      fromString getOrElse fromNumber
    }

    def writes(a: FiniteDuration) = JsString(a.toString)
  }


  implicit class WritesOps[A](val self: Writes[A]) extends AnyVal {
    def as[B <: A]: Writes[B] = self.contramap[B](identity)
  }
}
