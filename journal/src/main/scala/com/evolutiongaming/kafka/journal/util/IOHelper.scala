package com.evolutiongaming.kafka.journal.util

import cats.Applicative
import cats.effect.IO
import cats.implicits._
import cats.kernel.Monoid

object IOHelper {

  implicit val MonoidIOUnit: Monoid[IO[Unit]] = Applicative.monoid[IO, Unit]
}
