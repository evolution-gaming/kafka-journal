package com.evolutiongaming.kafka.journal

import cats.effect.Sync

private[journal] final case class HostName(value: String) {

  override def toString: String = value
}

private[journal] object HostName {

  def of[F[_]: Sync](): F[Option[HostName]] = {
    Sync[F].delay {
      for {
        a <- com.evolutiongaming.hostname.HostName()
      } yield {
        HostName(a)
      }
    }
  }
}
