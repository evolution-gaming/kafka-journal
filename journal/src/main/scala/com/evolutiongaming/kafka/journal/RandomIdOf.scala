package com.evolutiongaming.kafka.journal

import java.util.UUID

import cats.effect.Sync

trait RandomIdOf[F[_]] {

  def apply: F[RandomId]
}

object RandomIdOf {

  def apply[F[_]](implicit F: RandomIdOf[F]): RandomIdOf[F] = F


  def uuid[F[_] : Sync]: RandomIdOf[F] = new RandomIdOf[F] {

    def apply = Sync[F].delay { RandomId(UUID.randomUUID().toString) }
  }
}