package com.evolutiongaming.kafka.journal.util

import cats.Monad
import cats.effect._

object ConcurrentOf {

  def fromAsync[F[_]](implicit F: Async[F]): Concurrent[F] = {

    new Concurrent[F] {

      def start[A](fa: F[A]) = {
        F.map(fa) { a =>
          new Fiber[F, A] {
            def cancel = pure(())
            def join = pure(a)
          }
        }
      }

      def racePair[A, B](fa: F[A], fb: F[B]) = {
        F.flatMap(start(fa)) { fa =>
          F.flatMap(start(fb)) { fb =>
            F.map(fa.join) { a =>
              Left((a, fb))
            }
          }
        }
      }

      def async[A](k: (Either[Throwable, A] => Unit) => Unit) = F.async(k)

      def asyncF[A](k: (Either[Throwable, A] => Unit) => F[Unit]) = F.asyncF(k)

      def suspend[A](thunk: => F[A]) = F.defer(thunk)

      def bracketCase[A, B](acquire: F[A])(use: A => F[B])(release: (A, ExitCase[Throwable]) => F[Unit]) = {
        F.bracketCase(acquire)(use)(release)
      }
      def raiseError[A](e: Throwable) = F.raiseError(e)

      def handleErrorWith[A](fa: F[A])(f: Throwable => F[A]) = F.handleErrorWith(fa)(f)

      def flatMap[A, B](fa: F[A])(f: A => F[B]) = F.flatMap(fa)(f)

      def tailRecM[A, B](a: A)(f: A => F[Either[A, B]]) = F.tailRecM(a)(f)

      def pure[A](a: A) = F.pure(a)
    }
  }


  def fromMonad[F[_]](implicit F: Monad[F]): Concurrent[F] = {

    new Concurrent[F] {

      def start[A](fa: F[A]) = {
        F.map(fa) { a =>
          new Fiber[F, A] {
            def cancel = pure(())
            def join = pure(a)
          }
        }
      }

      def racePair[A, B](fa: F[A], fb: F[B]) = {
        F.flatMap(start(fa)) { fa =>
          F.flatMap(start(fb)) { fb =>
            F.map(fa.join) { a =>
              Left((a, fb))
            }
          }
        }
      }

      def async[A](k: (Either[Throwable, A] => Unit) => Unit) = throw new IllegalArgumentException("async")

      def asyncF[A](k: (Either[Throwable, A] => Unit) => F[Unit]) = throw new IllegalArgumentException("asyncF")

      def suspend[A](thunk: => F[A]) = thunk

      def bracketCase[A, B](acquire: F[A])(use: A => F[B])(release: (A, ExitCase[Throwable]) => F[Unit]) = {
        F.flatMap(acquire) { a =>
          F.flatMap(use(a)) { b =>
            F.map(release(a, ExitCase.complete)) { _ => b }
          }
        }
      }

      def raiseError[A](e: Throwable) = throw e

      def handleErrorWith[A](fa: F[A])(f: Throwable => F[A]) = fa

      def flatMap[A, B](fa: F[A])(f: A => F[B]) = F.flatMap(fa)(f)

      def tailRecM[A, B](a: A)(f: A => F[Either[A, B]]) = F.tailRecM(a)(f)

      def pure[A](a: A) = F.pure(a)
    }
  }
}
