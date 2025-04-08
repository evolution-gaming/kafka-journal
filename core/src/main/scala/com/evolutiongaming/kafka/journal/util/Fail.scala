package com.evolutiongaming.kafka.journal.util

import cats.Id
import cats.effect.IO
import cats.syntax.all.*
import com.evolutiongaming.catshelper.ApplicativeThrowable
import com.evolutiongaming.kafka.journal.JournalError
import play.api.libs.json.{JsError, JsResult}
import scodec.{Attempt, Err}

import scala.util.{Failure, Try}

trait Fail[F[_]] {

  def fail[A](a: String): F[A]
}

object Fail {

  def apply[F[_]](
    implicit
    F: Fail[F],
  ): Fail[F] = F

  implicit val idFail: Fail[Id] = new Fail[Id] {
    def fail[A](a: String): Id[A] = throw JournalError(a)
  }

  implicit val jsResultFail: Fail[JsResult] = new Fail[JsResult] {
    def fail[A](a: String): JsResult[A] = JsError(a)
  }

  implicit val attemptFail: Fail[Attempt] = new Fail[Attempt] {
    def fail[A](a: String): Attempt[A] = Attempt.Failure(Err(a))
  }

  implicit val optionFail: Fail[Option] = new Fail[Option] {
    def fail[A](a: String): Option[A] = none[A]
  }

  implicit val eitherFail: Fail[Either[String, *]] = new Fail[Either[String, *]] {
    def fail[A](a: String): Either[String, A] = a.asLeft[A]
  }

  implicit val tryFail: Fail[Try] = new Fail[Try] {
    def fail[A](a: String): Try[A] = Failure(JournalError(a))
  }

  implicit val ioFail: Fail[IO] = lift[IO]

  def lift[F[_]: ApplicativeThrowable]: Fail[F] = {
    new Fail[F] {
      def fail[A](a: String): F[A] = JournalError(a).raiseError[F, A]
    }
  }

  object implicits {

    implicit class StringOpsFail(val self: String) extends AnyVal {

      def fail[F[_]: Fail, A]: F[A] = Fail[F].fail(self)
    }
  }
}
