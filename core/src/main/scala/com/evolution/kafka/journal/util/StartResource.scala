package com.evolution.kafka.journal.util

import cats.effect.implicits.*
import cats.effect.{Deferred, *}
import cats.syntax.all.*

private[journal] object StartResource {

  def apply[F[_]: Concurrent, A, B](
    res: Resource[F, A],
  )(
    use: A => F[B],
  ): F[Fiber[F, Throwable, B]] = {

    res
      .allocated
      .bracketCase {
        case (a, release) =>
          for {
            released <- Deferred[F, Unit]
            fiber <- Concurrent[F].start {
              use(a).guarantee {
                release.guarantee {
                  released.complete(()).void
                }
              }
            }
          } yield {
            new Fiber[F, Throwable, B] {
              def cancel: F[Unit] = fiber.cancel *> released.get
              def join: F[Outcome[F, Throwable, B]] = fiber.join
            }
          }
      } {
        case ((_, release), exitCase) =>
          exitCase match {
            case Outcome.Succeeded(_) => ().pure[F]
            case Outcome.Errored(_) => release
            case Outcome.Canceled() => release
          }
      }
  }
}
