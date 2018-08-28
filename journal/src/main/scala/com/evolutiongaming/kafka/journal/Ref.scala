package com.evolutiongaming.kafka.journal

import java.util.concurrent.atomic.AtomicReference

trait Ref[A, F[_]] {

  def set(value: A): F[Unit]

  def get(): F[A]
}

object Ref {

  def apply[A, F[_] : IO](ref: AtomicReference[A]): Ref[A, F] = new Ref[A, F] {

    def set(value: A) = IO[F].point(ref.set(value))

    def get() = IO[F].point(ref.get())
  }

  def apply[A, F[_] : IO](): Ref[A, F] = apply[A, F](new AtomicReference[A])
}