package com.evolutiongaming.kafka.journal.util

import cats.effect.concurrent.Ref
import cats.effect.implicits._
import cats.effect.{Resource, Sync}
import cats.implicits._

trait ResourceRef[F[_], A] {

  def get: F[A]

  def set(a: A, release: F[Unit]): F[Unit]

  def set(a: Resource[F, A]): F[Unit]
}

object ResourceRef {

  def of[F[_] : Sync, A](resource: Resource[F, A]): Resource[F, ResourceRef[F, A]] = {

    case class State(a: A, release: F[Unit])

    Resource
      .make {
        for {
          ab           <- resource.allocated
          (a, release)  = ab
          ref          <- Ref[F].of(State(a, release))
        } yield ref
      } { ref =>
        ref.get.flatMap { _.release }
      }
      .map { ref =>
        new ResourceRef[F, A] {

          def get = {
            ref
              .get
              .map { _.a }
          }

          def set(a: A, release: F[Unit]) = {
            ref
              .getAndSet(State(a, release))
              .flatMap { _.release }
              .uncancelable
          }

          def set(a: Resource[F, A]) = {
            a
              .allocated
              .flatMap { case (a, release) => set(a, release) }
          }
        }
      }
  }
}