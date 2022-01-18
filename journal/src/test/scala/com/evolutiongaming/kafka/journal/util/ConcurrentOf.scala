package com.evolutiongaming.kafka.journal.util

import cats.Monad
import cats.effect._

import scala.annotation.nowarn
import scala.util.control.NonFatal

object ConcurrentOf {

  def fromAsync[F[_]](implicit F: Async[F]): Concurrent[F] = F

  def fromMonad[F[_]](implicit F: Monad[F]): Concurrent[F] = {
    new Concurrent[F] {

      override def ref[A](a: A): F[Ref[F, A]] = ???

      override def deferred[A]: F[Deferred[F, A]] = ???

      override def start[A](fa: F[A]): F[Fiber[F, Throwable, A]] =
        F.map(fa) { a =>
          new Fiber[F, Throwable, A] {
            def cancel = pure(())
            def join = pure(Outcome.succeeded(pure(a)))
          }
        }

      override def never[A]: F[A] = ???

      override def cede: F[Unit] = F.unit

      override def forceR[A, B](fa: F[A])(fb: F[B]): F[B] = {
        try { fa } catch { case NonFatal(_) => () }
        fb
      }

      override def uncancelable[A](body: Poll[F] => F[A]): F[A] = body(new Poll[F] {
        override def apply[X](fa: F[X]): F[X] = fa
      })

      override def canceled: F[Unit] = ???

      override def onCancel[A](fa: F[A], fin: F[Unit]): F[A] = ???

      override def raiseError[A](e: Throwable): F[A] = throw e

      override def handleErrorWith[A](fa: F[A])(f: Throwable => F[A]): F[A] =
        try { fa } catch { case NonFatal(e) => f(e) }

      override def flatMap[A, B](fa: F[A])(f: A => F[B]): F[B] =
        F.flatMap(fa)(f)

      override def tailRecM[A, B](a: A)(f: A => F[Either[A, B]]): F[B] =
        F.tailRecM(a)(f)

      override def pure[A](x: A): F[A] = F.pure(x)

      override def unique: F[Unique.Token] = F.pure(new Unique.Token())
    }
  }
}
