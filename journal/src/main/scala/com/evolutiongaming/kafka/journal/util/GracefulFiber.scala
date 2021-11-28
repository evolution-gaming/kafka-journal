package com.evolutiongaming.kafka.journal.util

import cats.effect.{Concurrent, Fiber}
import cats.syntax.all._
import cats.effect.Ref


trait GracefulFiber[F[_]] {
  def apply[A](f: F[Boolean] => F[Fiber[F, A]]): F[Fiber[F, A]]
}

object GracefulFiber {

  def apply[F[_] : Concurrent]: GracefulFiber[F] = {

    new GracefulFiber[F] {

      def apply[A](f: F[Boolean] => F[Fiber[F, A]]) = {
        for {
          cancelRef <- Ref.of[F, Boolean](false)
          fiber     <- f(cancelRef.get)
        } yield {
          new Fiber[F, A] {

            def join = fiber.join

            def cancel = {
              for {
                cancel <- cancelRef.getAndSet(true)
                _      <- if (cancel) ().pure[F] else fiber.join
              } yield {}
            }
          }
        }
      }
    }
  }
}
