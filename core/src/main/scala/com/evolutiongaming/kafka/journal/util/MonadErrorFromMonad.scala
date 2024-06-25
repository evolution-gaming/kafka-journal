package com.evolutiongaming.kafka.journal.util

import cats.{Monad, MonadError}

trait MonadErrorFromMonad[F[_], E] extends MonadError[F, E] {

  def F: Monad[F]

  def flatMap[A, B](fa: F[A])(f: A => F[B]) = F.flatMap(fa)(f)

  def tailRecM[A, B](a: A)(f: A => F[Either[A, B]]) = F.tailRecM(a)(f)

  def pure[A](a: A) = F.pure(a)
}
