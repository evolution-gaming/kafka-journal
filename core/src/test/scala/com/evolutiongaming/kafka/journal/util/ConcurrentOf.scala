package com.evolutiongaming.kafka.journal.util

import cats.Monad
import cats.effect.kernel.{Async, Outcome, Unique}
import cats.effect.{Concurrent, Fiber, Poll}
import cats.syntax.all._

import scala.util.control.NonFatal

object ConcurrentOf {

  def fromAsync[F[_]](implicit F: Async[F]): Concurrent[F] = F

  def fromMonad[F[_]](implicit F: Monad[F]): Concurrent[F] = {
    new Concurrent[F] {

      def ref[A](a: A) = throw new NotImplementedError(s"Concurrent.ref")

      def deferred[A] = throw new NotImplementedError(s"Concurrent.deferred")

      def start[A](fa: F[A]) =
        F.map(fa) { a =>
          new Fiber[F, Throwable, A] {
            def cancel = pure(())
            def join   = pure(Outcome.succeeded(pure(a)))
          }
        }

      def never[A] = {
        throw new NotImplementedError(s"Concurrent.never")
      }

      def cede: F[Unit] = F.unit

      def forceR[A, B](fa: F[A])(fb: F[B]): F[B] = {
        try { fa }
        catch { case NonFatal(_) => () }
        fb
      }

      def uncancelable[A](body: Poll[F] => F[A]): F[A] = body {
        new Poll[F] {
          def apply[X](fa: F[X]) = fa
        }
      }

      def canceled = throw new NotImplementedError(s"Concurrent.canceled")

      def onCancel[A](fa: F[A], fin: F[Unit]) = fa.productL(fin)

      def raiseError[A](e: Throwable): F[A] = throw e

      def handleErrorWith[A](fa: F[A])(f: Throwable => F[A]): F[A] =
        try { fa }
        catch { case NonFatal(e) => f(e) }

      def flatMap[A, B](fa: F[A])(f: A => F[B]): F[B] =
        F.flatMap(fa)(f)

      def tailRecM[A, B](a: A)(f: A => F[Either[A, B]]): F[B] =
        F.tailRecM(a)(f)

      def pure[A](x: A): F[A] = F.pure(x)

      def unique = F.pure(new Unique.Token())
    }
  }
}
