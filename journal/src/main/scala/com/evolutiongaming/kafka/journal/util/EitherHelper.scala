package com.evolutiongaming.kafka.journal.util

import cats.implicits._

import scala.util.Either

object EitherHelper {

  private val RightUnit = ().asRight

  private val LeftUnit = ().asLeft


  implicit class EitherBooleanOps(val self: Boolean) extends AnyVal {

    def trueOr[A](a: => A): Either[A, Unit] = {
      if (self) Right.unit else a.asLeft
    }

    def falseOr[A](a: => A): Either[A, Unit] = {
      (!self).trueOr(a)
    }
  }


  implicit class LeftOps(val self: Left.type) extends AnyVal {

    def unit[A]: Either[Unit, A] = LeftUnit
  }


  implicit class RightOps(val self: Right.type) extends AnyVal {

    def unit[A]: Either[A, Unit] = RightUnit
  }
}
