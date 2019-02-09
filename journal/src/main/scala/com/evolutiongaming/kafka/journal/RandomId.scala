package com.evolutiongaming.kafka.journal

import java.util.UUID

import cats.effect.Sync

trait RandomId[F[_]] {

  def get: F[String]
}

object RandomId {

  def apply[F[_]](implicit F: RandomId[F]): RandomId[F] = F


  def uuid[F[_] : Sync]: RandomId[F] = new RandomId[F] {

    def get = Sync[F].delay { UUID.randomUUID().toString }
  }
}