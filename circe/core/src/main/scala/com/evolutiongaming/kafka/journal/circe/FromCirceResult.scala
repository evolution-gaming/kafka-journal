package com.evolutiongaming.kafka.journal.circe

import cats.syntax.all._
import com.evolutiongaming.catshelper.ApplicativeThrowable
import com.evolutiongaming.kafka.journal.JournalError
import io.circe.Error

trait FromCirceResult[F[_]] {

  def apply[A](result: Either[Error, A]): F[A]

}

object FromCirceResult {

  def summon[F[_]](implicit fromCirceResult: FromCirceResult[F]): FromCirceResult[F] = fromCirceResult

  implicit def lift[F[_]: ApplicativeThrowable]: FromCirceResult[F] = new FromCirceResult[F] {

    def apply[A](fa: Either[Error, A]): F[A] =
      fa.fold(
        a => JournalError(s"FromCirceResult failed: $a", a).raiseError[F, A],
        a => a.pure[F],
      )
  }

}
