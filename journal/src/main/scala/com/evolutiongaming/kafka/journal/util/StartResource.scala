package com.evolutiongaming.kafka.journal.util

import cats.effect._
import cats.effect.implicits._
import cats.syntax.all._
import cats.effect.Deferred

object StartResource {

  def apply[F[_] : Concurrent, A, B](
    res: Resource[F, A])(
    use: A => F[B]
  ): F[Fiber[F, B]] = {

    res.allocated.bracketCase { case (a, release) =>
      for {
        released <- Deferred[F, Unit]
        fiber    <- Concurrent[F].start {
          use(a).guarantee {
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
    } { case ((_, release), exitCase) =>
      exitCase match {
        case ExitCase.Completed           => ().pure[F]
        case _: ExitCase.Error[Throwable] => release
        case ExitCase.Canceled            => release
      }
    }
  }
}