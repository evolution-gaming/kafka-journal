package com.evolutiongaming.kafka.journal.util

import cats.effect.Clock

object ClockOf {
  def apply[F[_]](implicit F: Clock[F]): Clock[F] = F
}
