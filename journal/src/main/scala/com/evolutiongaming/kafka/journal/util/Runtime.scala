package com.evolutiongaming.kafka.journal.util

import java.lang.{Runtime => RuntimeJ}

import cats.effect.Sync

trait Runtime[F[_]] {
  def availableCores: F[Int]
}

object Runtime {

  def apply[F[_]](implicit F: Runtime[F]): Runtime[F] = F

  def apply[F[_] : Sync](runtime: RuntimeJ): Runtime[F] = {

    new Runtime[F] {
      def availableCores = Sync[F].delay { runtime.availableProcessors() }
    }
  }

  def lift[F[_] : Sync]: Runtime[F] = apply(RuntimeJ.getRuntime)
}
