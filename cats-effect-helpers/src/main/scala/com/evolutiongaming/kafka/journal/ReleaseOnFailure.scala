package com.evolutiongaming.kafka.journal

import cats.effect.{Bracket, ExitCase}

object ReleaseOnFailure {

  def apply[F[_], A, B, E](fa: F[A])(f: A => F[B])(release: A => F[Unit])(implicit bracket: Bracket[F, E]): F[B] = {
    Bracket[F, E].bracketCase(fa)(f) {
      case (_, ExitCase.Completed) => Bracket[F, E].unit
      case (a, ExitCase.Error(_))  => release(a)
      case (a, ExitCase.Canceled)  => release(a)
    }
  }
}
