package com.evolutiongaming.kafka.journal

import cats.Monad
import com.evolutiongaming.catshelper.MonadThrowable

import scala.util.control.NonFatal

object MonadThrowableOf {

  def apply[F[_]](implicit F: Monad[F]): MonadThrowable[F] = new MonadThrowable[F] {

    def flatMap[A, B](fa: F[A])(f: A => F[B]) = F.flatMap(fa)(f)

    def tailRecM[A, B](a: A)(f: A => F[Either[A, B]]) = F.tailRecM(a)(f)

    def raiseError[A](e: Throwable) = throw e

    def handleErrorWith[A](fa: F[A])(f: Throwable => F[A]) = try fa catch { case NonFatal(e) => f(e) }

    def pure[A](a: A) = F.pure(a)
  }
}
