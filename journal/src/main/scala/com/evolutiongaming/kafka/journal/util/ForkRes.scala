package com.evolutiongaming.kafka.journal.util

import cats.effect.concurrent.Deferred
import cats.effect.implicits._
import cats.effect.{Concurrent, ContextShift, Fiber, Resource}
import cats.implicits._

object ForkRes {

  def apply[F[_] : Concurrent : ContextShift, A, B](
    res: Resource[F, A])(
    use: A => F[B]): F[Fiber[F, B]] = {
    
    for {
      allocated    <- res.allocated
      (a, release)  = allocated
      released     <- Deferred[F, Unit]
      fiber        <- Concurrent[F].start {
        (/*TODO*/ContextShift[F].shift *> use(a)).guarantee {
          release.guarantee {
            released.complete(())
          }
        }
      }
    } yield {
      new Fiber[F, B] {
        def cancel = fiber.cancel *> released.get
        def join = fiber.join
      }
    }
  }
}
