package com.evolution.kafka.journal.util

import cats.effect.syntax.all.*
import cats.effect.{Ref, Resource, Sync}
import cats.syntax.all.*

import scala.util.control.NoStackTrace

private[journal] trait ResourceRef[F[_], A] {

  def get: F[A]

  def set(a: A, release: F[Unit]): F[Unit]

  def set(a: Resource[F, A]): F[Unit]
}

private[journal] object ResourceRef {

  def make[F[_]: Sync, A](resource: Resource[F, A]): Resource[F, ResourceRef[F, A]] = {

    case class State(a: A, release: F[Unit])

    Resource
      .make {
        for {
          ab <- resource.allocated
          (a, release) = ab
          ref <- Ref[F].of(State(a, release).some)
        } yield ref
      } { ref =>
        ref
          .getAndSet(none)
          .flatMap { _.foldMapM { _.release } }
      }
      .map { ref =>
        new ResourceRef[F, A] {

          def get: F[A] = {
            ref
              .get
              .flatMap {
                case Some(state) => state.a.pure[F]
                case None => ResourceReleasedError.raiseError[F, A]
              }
          }

          def set(a: A, release: F[Unit]): F[Unit] = {
            ref
              .modify {
                case Some(state) => (State(a, release).some, state.release)
                case None => (none, ResourceReleasedError.raiseError[F, Unit])
              }
              .flatten
              .uncancelable
          }

          def set(a: Resource[F, A]): F[Unit] = {
            a
              .allocated
              .flatMap { case (a, release) => set(a, release) }
          }
        }
      }
  }
}

case object ResourceReleasedError extends RuntimeException("Resource released") with NoStackTrace
