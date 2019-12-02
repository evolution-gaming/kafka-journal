package com.evolutiongaming.kafka.journal.util

import cats.MonadError
import cats.data.{NonEmptyList => Nel}
import com.evolutiongaming.scassandra.{DecodeByName, EncodeByName}
import play.api.libs.json._

import scala.concurrent.duration._
import scala.util.Try


object PlayJsonHelper {

  implicit val jsValueEncodeByName: EncodeByName[JsValue] = encodeByNameFromWrites

  implicit val jsValueDecodeByName: DecodeByName[JsValue] = decodeByNameFromReads


  implicit val jsValueOptEncodeByName: EncodeByName[Option[JsValue]] = EncodeByName.optEncodeByName

  implicit val jsValueOptDecodeByName: DecodeByName[Option[JsValue]] = DecodeByName.optDecodeByName


  implicit val jsErrorFromStr: FromStr[JsError] = JsError(_)

  implicit val jsErrorToStr: ToStr[JsError] = _.errors.toString()


  implicit val jsResultMonadThrowable: MonadError[JsResult, JsError] = new MonadError[JsResult, JsError] {

    def raiseError[A](a: JsError) = a

    def handleErrorWith[A](fa: JsResult[A])(f: JsError => JsResult[A]) = {
      fa match {
        case fa: JsSuccess[A] => fa
        case fa: JsError      => f(fa)
      }
    }

    def pure[A](a: A) = JsSuccess(a)

    def flatMap[A, B](fa: JsResult[A])(f: A => JsResult[B]) = fa.flatMap(f)

    def tailRecM[A, B](a: A)(f: A => JsResult[Either[A, B]]): JsResult[B] = {
      f(a) match {
        case b: JsError                 => b
        case b: JsSuccess[Either[A, B]] => b.value match {
          case Left(b1) => tailRecM(b1)(f)
          case Right(a) => JsSuccess(a)
        }
      }
    }
  }


  implicit val jsResultMonadString: MonadString[JsResult] = MonadString[JsResult, JsError]


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


  def encodeByNameFromWrites[A](implicit writes: Writes[A]): EncodeByName[A] = EncodeByName[String].contramap { a =>
    val jsValue = writes.writes(a)
    Json.stringify(jsValue)
  }

  
  def decodeByNameFromReads[A](implicit reads: Reads[A]): DecodeByName[A] = DecodeByName[String].map { a =>
    val jsValue = Json.parse(a)
    // TODO not use `as`
    jsValue.as(reads)
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
