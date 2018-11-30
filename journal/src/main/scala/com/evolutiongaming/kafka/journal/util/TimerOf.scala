package com.evolutiongaming.kafka.journal.util

import cats.effect.Timer

object TimerOf {
  def apply[F[_]](implicit F: Timer[F]): Timer[F] = F
}
