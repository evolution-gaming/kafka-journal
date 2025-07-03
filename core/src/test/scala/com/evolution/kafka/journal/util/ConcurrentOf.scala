package com.evolution.kafka.journal.util

import cats.Monad
import cats.effect.kernel.{Async, Deferred, Outcome, Ref, Unique}
import cats.effect.{Concurrent, Fiber, Poll}
import cats.syntax.all.*

import scala.util.control.NonFatal

object ConcurrentOf {

  def fromAsync[F[_]](
    implicit
    F: Async[F],
  ): Concurrent[F] = F

  def fromMonad[F[_]](
    implicit
    F: Monad[F],
  ): Concurrent[F] = {
    new Concurrent[F] {

      def ref[A](a: A): F[Ref[F, A]] = throw new NotImplementedError(s"Concurrent.ref")

      def deferred[A]: F[Deferred[F, A]] = throw new NotImplementedError(s"Concurrent.deferred")

      def start[A](fa: F[A]): F[Fiber[F, Throwable, A]] =
        F.map(fa) { a =>
          new Fiber[F, Throwable, A] {
            def cancel: F[Unit] = pure(())
            def join: F[Outcome[F, Throwable, A]] = pure(Outcome.succeeded(pure(a)))
          }
        }

      def never[A]: F[A] = {
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
          def apply[X](fa: F[X]): F[X] = fa
        }
      }

      def canceled: F[Unit] = throw new NotImplementedError(s"Concurrent.canceled")

      def onCancel[A](fa: F[A], fin: F[Unit]): F[A] = fa.productL(fin)

      def raiseError[A](e: Throwable): F[A] = throw e

      def handleErrorWith[A](fa: F[A])(f: Throwable => F[A]): F[A] =
        try { fa }
        catch { case NonFatal(e) => f(e) }

      def flatMap[A, B](fa: F[A])(f: A => F[B]): F[B] =
        F.flatMap(fa)(f)

      def tailRecM[A, B](a: A)(f: A => F[Either[A, B]]): F[B] =
        F.tailRecM(a)(f)

      def pure[A](x: A): F[A] = F.pure(x)

      def unique: F[Unique.Token] = F.pure(new Unique.Token())
    }
  }
}
