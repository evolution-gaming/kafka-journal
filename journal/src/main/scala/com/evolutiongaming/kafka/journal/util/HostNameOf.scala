package com.evolutiongaming.kafka.journal.util

import cats.effect.Sync
import com.evolutiongaming.hostname.HostName

object HostNameOf {

  def apply[F[_] : Sync]: F[Option[String]] = {
    Sync[F].delay { HostName() }
  }
}
