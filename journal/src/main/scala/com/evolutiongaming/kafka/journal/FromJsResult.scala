package com.evolutiongaming.kafka.journal

import cats.effect.IO
import cats.implicits._
import com.evolutiongaming.catshelper.ApplicativeThrowable
import play.api.libs.json.{JsResult, JsResultException}

import scala.util.Try


trait FromJsResult[F[_]] {

  def apply[A](fa: JsResult[A]): F[A]
}

object FromJsResult {

  def apply[F[_]](implicit F: FromJsResult[F]): FromJsResult[F] = F


  def lift[F[_]: ApplicativeThrowable]: FromJsResult[F] = new FromJsResult[F] {

    def apply[A](fa: JsResult[A]) = {
      fa.fold(
        a => JournalError(s"FromJsResult failed: $a", JsResultException(a)).raiseError[F, A],
        a => a.pure[F])
    }
  }


  implicit val tryFromAttempt: FromJsResult[Try] = lift

  implicit val ioFromJsResult: FromJsResult[IO] = lift
}
