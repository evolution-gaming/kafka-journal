package com.evolutiongaming.kafka.journal.util

import cats.Applicative
import cats.effect.IO
import cats.implicits._
import cats.kernel.Monoid

object IOHelper {

  // TODO remove and not pass as argument
  implicit val MonoidIOUnit: Monoid[IO[Unit]] = Applicative.monoid[IO, Unit]
}
