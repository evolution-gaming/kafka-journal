package com.evolutiongaming.kafka.journal

import cats.Applicative
import cats.effect.IO
import cats.syntax.all.*
import com.evolutiongaming.kafka.journal.util.Fail
import com.evolutiongaming.kafka.journal.util.Fail.implicits.*
import scodec.Attempt

import scala.util.Try

trait FromAttempt[F[_]] {

  def apply[A](fa: Attempt[A]): F[A]
}

object FromAttempt {

  def apply[F[_]](
    implicit
    F: FromAttempt[F],
  ): FromAttempt[F] = F

  def lift[F[_]: Applicative: Fail]: FromAttempt[F] = new FromAttempt[F] {

    def apply[A](fa: Attempt[A]) = {
      fa.fold(a => s"scodec error ${ a.messageWithContext }".fail[F, A], a => a.pure[F])
    }
  }

  implicit val tryFromAttempt: FromAttempt[Try] = lift[Try]

  implicit val ioFromAttempt: FromAttempt[IO] = lift[IO]
}
