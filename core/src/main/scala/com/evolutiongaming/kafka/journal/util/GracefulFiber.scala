package com.evolutiongaming.kafka.journal.util

import cats.effect.{Concurrent, Fiber, Ref}
import cats.syntax.all.*

trait GracefulFiber[F[_]] {
  def apply[A](f: F[Boolean] => F[Fiber[F, Throwable, A]]): F[Fiber[F, Throwable, A]]
}

object GracefulFiber {

  def apply[F[_]: Concurrent]: GracefulFiber[F] = {

    new GracefulFiber[F] {

      def apply[A](f: F[Boolean] => F[Fiber[F, Throwable, A]]) = {
        for {
          cancelRef <- Ref.of[F, Boolean](false)
          fiber     <- f(cancelRef.get)
        } yield {
          new Fiber[F, Throwable, A] {

            def join = fiber.join

            def cancel = {
              for {
                cancel <- cancelRef.getAndSet(true)
                _      <- if (cancel) ().pure[F] else fiber.joinWithNever
              } yield {}
            }
          }
        }
      }
    }
  }
}
