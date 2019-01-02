package com.evolutiongaming.kafka.journal.util

import cats.effect.concurrent.Ref
import cats.effect.{Concurrent, Fiber}
import cats.implicits._


// TODO rename
trait FiberAndCancel[F[_]] {
  def apply[A](f: F[Boolean] => F[Fiber[F, A]]): F[Fiber[F, A]]
}

object FiberAndCancel {

  def apply[F[_] : Concurrent]: FiberAndCancel[F] = {

    new FiberAndCancel[F] {

      def apply[A](f: F[Boolean] => F[Fiber[F, A]]) = {
        for {
          cancelRef <- Ref.of[F, Boolean](false)
          fiber     <- f(cancelRef.get)
        } yield {
          new Fiber[F, A] {
            def cancel = {
              for {
                _ <- cancelRef.set(true)
                r <- fiber.cancel
              } yield r
            }

            def join = fiber.join
          }
        }
      }
    }
  }
}
