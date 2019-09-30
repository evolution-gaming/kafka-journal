package com.evolutiongaming.kafka.journal.util

import cats.MonadError

trait BracketFromMonadError[F[_], E] extends BracketFromMonad[F, E] {

  def F: MonadError[F, E]

  def raiseError[A](e: E) = F.raiseError(e)

  def handleErrorWith[A](fa: F[A])(f: E => F[A]) = F.handleErrorWith(fa)(f)
}
