package com.evolutiongaming.kafka.journal

import cats.effect.{ExitCase, Sync}
import com.evolutiongaming.kafka.journal.CatsHelper.MonadErrorE

object SyncOf {

  def apply[F[_]](implicit F: MonadErrorE[F], S: Suspend[F], B: BracketCase[F]): Sync[F] = new Sync[F] {

    def suspend[A](thunk: => F[A]) = S.suspend(thunk)

    def bracketCase[A, B](acquire: F[A])(use: A => F[B])(release: (A, ExitCase[Throwable]) => F[Unit]) = {
      B.bracketCase(acquire)(use)(release)
    }

    def raiseError[A](e: Throwable) = F.raiseError(e)

    def handleErrorWith[A](fa: F[A])(f: Throwable => F[A]) = F.handleErrorWith(fa)(f)

    def flatMap[A, B](fa: F[A])(f: A => F[B]) = F.flatMap(fa)(f)

    def tailRecM[A, B](a: A)(f: A => F[Either[A, B]]) = F.tailRecM(a)(f)

    def pure[A](a: A) = F.pure(a)
  }
}
