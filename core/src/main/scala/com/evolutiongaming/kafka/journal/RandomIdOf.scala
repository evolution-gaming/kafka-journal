package com.evolutiongaming.kafka.journal

import cats.effect.Sync

import java.util.UUID

trait RandomIdOf[F[_]] {

  def apply: F[RandomId]
}

object RandomIdOf {

  def apply[F[_]](implicit F: RandomIdOf[F]): RandomIdOf[F] = F

  def uuid[F[_]: Sync]: RandomIdOf[F] = new RandomIdOf[F] {

    def apply = Sync[F].delay(RandomId(UUID.randomUUID().toString))
  }
}
