package com.evolutiongaming.kafka.journal.util

import cats.{Applicative, CommutativeApplicative}

object CommutativeApplicativeOf {

  def apply[F[_]](applicative: Applicative[F]): CommutativeApplicative[F] = new CommutativeApplicative[F] {

    def pure[A](x: A) = applicative.pure(x)

    def ap[A, B](ff: F[A => B])(fa: F[A]) = applicative.ap(ff)(fa)
  }
}
