package com.evolution.kafka.journal.util

import cats.MonadError
import cats.data.NonEmptyList as Nel
import play.api.libs.json.*

import scala.annotation.tailrec
import scala.concurrent.duration.*
import scala.util.Try

private[journal] object PlayJsonHelper {

  implicit val jsResultMonadError: MonadError[JsResult, JsError] = new MonadError[JsResult, JsError] {

    def raiseError[A](a: JsError): JsResult[A] = a

    def handleErrorWith[A](fa: JsResult[A])(f: JsError => JsResult[A]): JsResult[A] = {
      fa match {
        case fa: JsSuccess[A @unchecked] => fa
        case fa: JsError => f(fa)
      }
    }

    def pure[A](a: A): JsResult[A] = JsSuccess(a)

    def flatMap[A, B](fa: JsResult[A])(f: A => JsResult[B]): JsResult[B] = fa.flatMap(f)

    @tailrec
    def tailRecM[A, B](a: A)(f: A => JsResult[Either[A, B]]): JsResult[B] = {
      f(a) match {
        case b: JsSuccess[Either[A, B] @unchecked] =>
          b.value match {
            case Right(a) => JsSuccess(a)
            case Left(b) => tailRecM(b)(f)
          }
        case b: JsError => b
      }
    }
  }

  implicit def nelReads[T](
    implicit
    reads: Reads[List[T]],
  ): Reads[Nel[T]] = {
    reads.mapResult {
      case Nil => JsError("list is empty")
      case head :: tail => JsSuccess(Nel(head, tail))
    }
  }

  implicit def nelWrites[A](
    implicit
    writes: Writes[List[A]],
  ): Writes[Nel[A]] = {
    writes.contramap(_.toList)
  }

  implicit class ReadsOps[A](val self: Reads[A]) extends AnyVal {

    final def mapResult[B](f: A => JsResult[B]): Reads[B] = (a: JsValue) => self.reads(a).flatMap(f)
  }

  implicit val finiteDurationFormat: Format[FiniteDuration] = new Format[FiniteDuration] {

    def reads(json: JsValue): JsResult[FiniteDuration] = {
      def fromString = for {
        str <- json.validate[String]
      } yield {
        Try { Duration(str).asInstanceOf[FiniteDuration] }
          .fold[JsResult[FiniteDuration]](a => JsError(s"cannot parse FiniteDuration from $str: $a"), a => JsSuccess(a))
      }

      def fromNumber = for {
        a <- json.validate[Long]
      } yield {
        a.millis
      }

      fromString getOrElse fromNumber
    }

    def writes(a: FiniteDuration): JsValue = JsString(a.toString)
  }

  implicit class WritesOps[A](val self: Writes[A]) extends AnyVal {
    def as[B <: A]: Writes[B] = self.contramap[B](identity)
  }
}
