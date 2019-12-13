package com.evolutiongaming.kafka.journal.eventual.cassandra


import cats.implicits._
import cats.kernel.Eq
import cats.{Order, Show}
import com.evolutiongaming.kafka.journal.util.ApplicativeString
import com.evolutiongaming.kafka.journal.util.OptionHelper._
import com.evolutiongaming.kafka.journal.util.TryHelper._

import scala.util.Try

sealed abstract case class Segments(value: Int) {

  override def toString: String = value.toString
}

object Segments {

  val min: Segments = new Segments(1) {}

  val max: Segments = new Segments(Int.MaxValue) {}

  val default: Segments = new Segments(100) {}


  implicit val eqSegments: Eq[Segments] = Eq.fromUniversalEquals

  implicit val showSegments: Show[Segments] = Show.fromToString


  implicit val orderingSegments: Ordering[Segments] = Ordering.by(_.value)

  implicit val orderSegments: Order[Segments] = Order.fromOrdering


  def of[F[_] : ApplicativeString](value: Int): F[Segments] = {
    if (value < min.value) {
      s"invalid Segments of $value, it must be greater or equal to $min".raiseError[F, Segments]
    } else if (value > max.value) {
      s"invalid Segments of $value, it must be less or equal to $max".raiseError[F, Segments]
    } else if (value === min.value) {
      min.pure[F]
    } else if (value === max.value) {
      max.pure[F]
    } else {
      new Segments(value) {}.pure[F]
    }
  }


  def opt(value: Int): Option[Segments] = of[Option](value)


  def unsafe[A](value: A)(implicit numeric: Numeric[A]): Segments = of[Try](numeric.toInt(value)).get
}
