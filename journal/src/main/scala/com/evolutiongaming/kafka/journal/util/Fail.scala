package com.evolutiongaming.kafka.journal.util

import cats.Id
import cats.implicits._
import com.evolutiongaming.catshelper.ApplicativeThrowable
import com.evolutiongaming.kafka.journal.JournalError
import play.api.libs.json.{JsError, JsResult}
import scodec.{Attempt, Err}

trait Fail[F[_]] {

  def fail[A](a: String): F[A]
}

object Fail {

  implicit val idFail: Fail[Id] = new Fail[Id] {
    def fail[A](a: String) = throw JournalError(a)
  }

  implicit val jsResultFail: Fail[JsResult] = new Fail[JsResult] {
    def fail[A](a: String) = JsError(a)
  }

  implicit val attemptFail: Fail[Attempt] = new Fail[Attempt] {
    def fail[A](a: String) = Attempt.Failure(Err(a))
  }

  implicit val optionFail: Fail[Option] = new Fail[Option] {
    def fail[A](a: String) = none[A]
  }


  def apply[F[_]](implicit F: Fail[F]): Fail[F] = F


  implicit def fromApplicativeThrowable[F[_] : ApplicativeThrowable]: Fail[F] = {
    new Fail[F] {
      def fail[A](a: String) = JournalError(a).raiseError[F, A]
    }
  }


  object implicits {

    implicit class StringOpsFail(val self: String) extends AnyVal {

      def fail[F[_] : Fail, A]: F[A] = Fail[F].fail(self)
    }
  }
}
