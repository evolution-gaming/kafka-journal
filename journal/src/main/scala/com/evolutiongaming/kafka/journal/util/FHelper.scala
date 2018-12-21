package com.evolutiongaming.kafka.journal.util

import cats.FlatMap
import cats.implicits._

object FHelper {

  implicit class FOps[F[_], A](val self: F[A]) extends AnyVal {

    def unit(implicit flatMap: FlatMap[F]): F[Unit] = {
      for {_ <- self} yield {}
    }
  }
}
