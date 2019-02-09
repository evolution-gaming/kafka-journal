package com.evolutiongaming.kafka.journal

import cats.{Monad, MonadError}

import scala.util.control.NonFatal

object MonadErrorOf {

  def throwable[F[_]](implicit F: Monad[F]): MonadError[F, Throwable] = new MonadError[F, Throwable] {

    def flatMap[A, B](fa: F[A])(f: A => F[B]) = F.flatMap(fa)(f)

    def tailRecM[A, B](a: A)(f: A => F[Either[A, B]]) = F.tailRecM(a)(f)

    def raiseError[A](e: Throwable) = throw e

    def handleErrorWith[A](fa: F[A])(f: Throwable => F[A]) = try fa catch { case NonFatal(e) => f(e) }

    def pure[A](a: A) = F.pure(a)
  }
}
