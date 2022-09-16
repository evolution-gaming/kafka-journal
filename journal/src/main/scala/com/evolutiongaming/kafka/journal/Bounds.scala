package com.evolutiongaming.kafka.journal

import cats.{ApplicativeThrow, Order, Semigroup}
import cats.syntax.all._

import scala.util.Try

sealed trait Bounds[+A]

object Bounds {
  implicit def semigroupBounds[A: Order]: Semigroup[Bounds[A]] = {
    (a: Bounds[A], b: Bounds[A]) => {
      Bounds
        .of[Try](
          min = a.min min b.min,
          max = a.max max b.max)
        .get
    }
  }


  def apply[A](value: A): Bounds[A] = new One(value) {}

  def of[F[_]]: OfApply[F] = new OfApply[F]()

  private[Bounds] final class OfApply[F[_]](val b: Boolean = false) extends AnyVal {
    def apply[A: Order](min: A, max: A)(implicit F: ApplicativeThrow[F]): F[Bounds[A]] = {
      if (min === max) {
        Bounds(min).pure[F]
      } else if (min > max) {
        JournalError(s"illegal arguments, `min` must be less or equal to `max`, min: $min, max: $max").raiseError[F, Bounds[A]]
      } else {
        val two: Bounds[A] = new Two(min, max) {}
        two.pure[F]
      }
    }
  }


  private sealed abstract case class One[A](value: A) extends Bounds[A] {
    override def toString = value.toString
  }

  private sealed abstract case class Two[A](min: A, max: A) extends Bounds[A] {
    override def toString = s"$min..$max"
  }

  implicit class BoundsOps[A](val self: Bounds[A]) extends AnyVal {

    def min: A = self match {
      case a: One[A] => a.value
      case a: Two[A] => a.min
    }

    def max: A = self match {
      case a: One[A] => a.value
      case a: Two[A] => a.max
    }

    def withMax[F[_]: ApplicativeThrow](a: A)(implicit order: Order[A]): F[Bounds[A]] = {
      Bounds.of[F](min = self.min, max = a)
    }

    def withMin[F[_]: ApplicativeThrow](a: A)(implicit order: Order[A]): F[Bounds[A]] = {
      Bounds.of[F](min = a, max = self.max)
    }

    def <(a: A)(implicit order: Order[A]): Boolean = self.max < a

    def >(a: A)(implicit order: Order[A]): Boolean = self.min > a

    def contains(a: A)(implicit order: Order[A]): Boolean = self match {
      case self: One[A] => self.value === a
      case self: Two[A] => self.min <= a && self.max >= a
    }
  }

  object implicits {
    implicit class Ops[A](val self: A) extends AnyVal {

      def <(bounds: Bounds[A])(implicit order: Order[A]): Boolean = self < bounds.min

      def >(bounds: Bounds[A])(implicit order: Order[A]): Boolean = self > bounds.max
    }
  }
}
