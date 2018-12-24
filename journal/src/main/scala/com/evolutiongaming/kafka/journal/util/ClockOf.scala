package com.evolutiongaming.kafka.journal.util

import java.util.concurrent.TimeUnit

import cats.Applicative
import cats.effect.Clock
import cats.implicits._

import scala.concurrent.duration.TimeUnit

object ClockOf {

  def apply[F[_]](implicit F: Clock[F]): Clock[F] = F


  def apply[F[_] : Applicative](millis: Long): Clock[F] = new Clock[F] {

    def realTime(unit: TimeUnit) = unit.convert(millis, TimeUnit.MILLISECONDS).pure[F]

    def monotonic(unit: TimeUnit) = realTime(unit)
  }
}
