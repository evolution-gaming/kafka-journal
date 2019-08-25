package com.evolutiongaming.kafka.journal

import cats.ApplicativeError
import cats.effect.IO
import cats.implicits._
import scodec.Attempt

import scala.util.Try

trait FromAttempt[F[_]] {

  def apply[A](fa: Attempt[A]): F[A]
}

object FromAttempt {

  def apply[F[_]](implicit F: FromAttempt[F]): FromAttempt[F] = F


  def lift[F[_]](implicit F: ApplicativeError[F, Throwable]): FromAttempt[F] = new FromAttempt[F] {

    def apply[A](fa: Attempt[A]) = {
      fa.fold(a => JournalError(s"scodec error ${ a.messageWithContext }").raiseError[F, A], _.pure[F])
    }
  }


  implicit val tryFromAttempt: FromAttempt[Try] = lift[Try]

  implicit val ioFromAttempt: FromAttempt[IO] = lift[IO]
}
