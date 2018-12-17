package com.evolutiongaming.kafka.journal

import java.util.concurrent.atomic.AtomicReference

trait AtomicRef[A, F[_]] {

  def set(value: A): F[Unit]

  def get(): F[A]
}

object AtomicRef {

  def apply[A, F[_] : IO2](ref: AtomicReference[A]): AtomicRef[A, F] = new AtomicRef[A, F] {

    def set(value: A) = {
      IO2[F].effect {
        ref.set(value)
      }
    }

    def get() = IO2[F].effect {
      ref.get()
    }
  }

  def apply[A, F[_] : IO2](): AtomicRef[A, F] = apply(new AtomicReference[A])
}