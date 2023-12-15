package com.evolutiongaming.kafka.journal.util

import cats.effect._
import cats.effect.Ref
import cats.effect.implicits._
import cats.syntax.all._

trait ResourceRegistry[F[_]] {

  def allocate[A](resource: Resource[F, A]): F[A]
}

object ResourceRegistry {

  def of[F[_]: Concurrent]: Resource[F, ResourceRegistry[F]] = {
    Resource
      .make {
        Ref.of[F, List[F[Unit]]](List.empty[F[Unit]])
      } { releases =>
        for {
          releases <- releases.get
          ignore    = (_: Throwable) => ()
          result   <- releases.foldMapM { _.handleError(ignore) }
        } yield result
      }
      .map { releases =>
        apply(releases)
      }
  }

  def apply[F[_] : Concurrent](releases: Ref[F, List[F[Unit]]]): ResourceRegistry[F] = {
    new ResourceRegistry[F] {
      def allocate[B](resource: Resource[F, B]) = {
        resource.allocated.bracketCase { case (b, release) =>
          for {
            _ <- releases.update(release :: _)
          } yield b
        } { case ((_, release), exitCase) =>
          exitCase match {
            case Outcome.Succeeded(_) => ().pure[F]
            case Outcome.Errored(_)   => release
            case Outcome.Canceled()   => release
          }
        }
      }
    }
  }
}