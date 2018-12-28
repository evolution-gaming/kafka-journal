package com.evolutiongaming.kafka.journal.util

import cats.Monad
import cats.effect.{ExitCase, Sync}

import scala.util.control.NonFatal

object TestSync {
  
  def apply[F[_]](implicit monad: Monad[F]): Sync[F] = {

    new Sync[F] {
      def suspend[A](thunk: => F[A]) = thunk

      def bracketCase[A, B](acquire: F[A])(use: A => F[B])(release: (A, ExitCase[Throwable]) => F[Unit]) = {
        flatMap(acquire) { a =>
          try {
            val b = use(a)
            try release(a, ExitCase.Completed) catch { case NonFatal(_) => }
            b
          } catch {
            case NonFatal(e) =>
              release(a, ExitCase.Error(e))
              raiseError(e)
          }
        }
      }

      def raiseError[A](e: Throwable) = throw e

      def handleErrorWith[A](fa: F[A])(f: Throwable => F[A]) = try fa catch { case NonFatal(e) => f(e) }

      def flatMap[A, B](fa: F[A])(f: A => F[B]) = monad.flatMap(fa)(f)

      def tailRecM[A, B](a: A)(f: A => F[Either[A, B]]) = monad.tailRecM(a)(f)

      def pure[A](a: A) = monad.pure(a)
    }
  }
}
