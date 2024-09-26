package com.evolutiongaming.kafka.journal.util

import cats.MonadError

private[journal] trait MonadCancelFromMonadError[F[_], E] extends MonadCancelFromMonad[F, E] {

  def F: MonadError[F, E]

  def raiseError[A](e: E) = F.raiseError(e)

  def handleErrorWith[A](fa: F[A])(f: E => F[A]) = F.handleErrorWith(fa)(f)
}
