package com.evolutiongaming.kafka.journal.util

import cats.effect._
import cats.effect.concurrent.Ref
import cats.effect.implicits._
import cats.implicits._
import cats.{Applicative, Foldable}

trait ResourceRegistry[F[_]] {

  def allocate[A](resource: Resource[F, A]): F[A]
}

object ResourceRegistry {

  def of[F[_] : Concurrent]: Resource[F, ResourceRegistry[F]] = {
    implicit val monoidUnit = Applicative.monoid[F, Unit]
    val result = for {
      releases <- Ref.of[F, List[F[Unit]]](List.empty[F[Unit]])
    } yield {
      val registry = apply[F](releases)
      val release = for {
        releases <- releases.get
        ignore    = (_: Throwable) => ()
        _        <- Foldable[List].foldMap(releases)(_.handleError(ignore))
      } yield {}
      (registry, release)
    }
    Resource(result)
  }

  def apply[F[_] : Sync](releases: Ref[F, List[F[Unit]]]): ResourceRegistry[F] = {
    new ResourceRegistry[F] {
      def allocate[B](resource: Resource[F, B]) = {
        resource.allocated.bracketCase { case (b, release) =>
          for {
            _ <- releases.update(release :: _)
          } yield b
        } { case ((_, release), exitCase) =>
          exitCase match {
            case ExitCase.Completed           => ().pure[F]
            case _: ExitCase.Error[Throwable] => release
            case ExitCase.Canceled            => release
          }
        }
      }
    }
  }
}