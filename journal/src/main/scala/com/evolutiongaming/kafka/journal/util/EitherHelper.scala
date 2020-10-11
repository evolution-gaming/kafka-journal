package com.evolutiongaming.kafka.journal.util

import cats.implicits._

import scala.util.Either

object EitherHelper {

  private val RightUnit = ().asRight

  private val LeftUnit = ().asLeft


  implicit class LeftOpsEitherHelper(val self: Left.type) extends AnyVal {

    def unit[A]: Either[Unit, A] = LeftUnit
  }


  implicit class RightOpsEitherHelper(val self: Right.type) extends AnyVal {

    def unit[A]: Either[A, Unit] = RightUnit
  }
}
