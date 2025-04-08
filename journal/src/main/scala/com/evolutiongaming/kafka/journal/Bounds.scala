package com.evolutiongaming.kafka.journal

import cats.syntax.all.*
import cats.{ApplicativeThrow, Order, Semigroup}

import scala.util.Try

/**
 * Bounded interval (also called inclusive range) of two values of type `A`.
 *
 * The interesting property of [[Bounds]] is that, when `combine` method is called with a second
 * interval, then larger interval is created, which includes both even if these intervals do not
 * overlap, i.e. including the gap between the intervals.
 *
 * Example:
 * {{{
 * val bounds1: Bounds[Int] = 10..20
 * val bounds2: Bounds[Int] = 50..70
 *
 * scala> import cats.syntax.all.*
 * scala> bounds1.combine(bounds2)
 * val res0: Bounds[Int] = 10..70
 * }}}
 */
private[journal] sealed trait Bounds[+A]

private[journal] object Bounds {
  implicit def semigroupBounds[A: Order]: Semigroup[Bounds[A]] = { (a: Bounds[A], b: Bounds[A]) =>
    {
      Bounds
        .of[Try](min = a.min min b.min, max = a.max max b.max)
        .get
    }
  }

  /**
   * Creates an interval containing a single value.
   *
   * I.e. for number `A`, it will create an interval `[A, A]`.
   *
   * @param value
   *   The only value in the interval.
   */
  def apply[A](value: A): Bounds[A] = new One(value) {}

  /**
   * Creates an interval from two values.
   *
   * I.e. for numbers `min` and `max`, it will create an interval `[min, max]`.
   *
   * @param min
   *   Start of the interval (including the value), must be less or equal to `max`
   * @param max
   *   End of the interval (including the value), must be greater or equal to `min`
   * @tparam A
   *   Type of the value, which can be ordered (i.e. have [[Order]] defined for it)
   * @tparam F
   *   An effect parameter allowing to raise error in case of the invalid interval
   */
  def of[F[_]]: OfApply[F] = new OfApply[F]()

  private[Bounds] final class OfApply[F[_]](val b: Boolean = false) extends AnyVal {
    def apply[A: Order](
      min: A,
      max: A,
    )(implicit
      F: ApplicativeThrow[F],
    ): F[Bounds[A]] = {
      if (min === max) {
        Bounds(min).pure[F]
      } else if (min > max) {
        JournalError(s"illegal arguments, `min` must be less or equal to `max`, min: $min, max: $max").raiseError[
          F,
          Bounds[A],
        ]
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

    /**
     * The first value included into interval
     */
    def min: A = self match {
      case a: One[A] => a.value
      case a: Two[A] => a.min
    }

    /**
     * The last value included into interval
     */
    def max: A = self match {
      case a: One[A] => a.value
      case a: Two[A] => a.max
    }

    /**
     * Create a new interval replacing the end of the interval by `a`
     */
    def withMax[F[_]: ApplicativeThrow](
      a: A,
    )(implicit
      order: Order[A],
    ): F[Bounds[A]] = {
      Bounds.of[F](min = self.min, max = a)
    }

    /**
     * Create a new interval replacing the start of the interval by `a`
     */
    def withMin[F[_]: ApplicativeThrow](
      a: A,
    )(implicit
      order: Order[A],
    ): F[Bounds[A]] = {
      Bounds.of[F](min = a, max = self.max)
    }

    /**
     * Checks if this interval is located entirely before a value `a`.
     *
     * @return
     *   `true` if `a` is greater than `y` for an interval `[x, y]`, `false` otherwise.
     */
    def <(
      a: A,
    )(implicit
      order: Order[A],
    ): Boolean = self.max < a

    /**
     * Checks if this interval is located entirely after a value `a`.
     *
     * @return
     *   `true` if `a` is less than `x` for an interval `[x, y]`, `false` otherwise.
     */
    def >(
      a: A,
    )(implicit
      order: Order[A],
    ): Boolean = self.min > a

    /**
     * Checks if a given value is within this interval.
     *
     * @return
     *   `true` if `a` is within an interval, or `false` otherwise
     */
    def contains(
      a: A,
    )(implicit
      order: Order[A],
    ): Boolean = self match {
      case self: One[A] => self.value === a
      case self: Two[A] => self.min <= a && self.max >= a
    }
  }

  object implicits {
    implicit class Ops[A](val self: A) extends AnyVal {

      /**
       * Checks if this value is located before an interval `bounds`.
       *
       * @return
       *   `true` if this value is less than `x` for an interval `[x, y]`, `false` otherwise.
       */
      def <(
        bounds: Bounds[A],
      )(implicit
        order: Order[A],
      ): Boolean = self < bounds.min

      /**
       * Checks if this value is located after an interval `bounds`.
       *
       * @return
       *   `true` if this value is greater than `y` for an interval `[x, y]`, `false` otherwise.
       */
      def >(
        bounds: Bounds[A],
      )(implicit
        order: Order[A],
      ): Boolean = self > bounds.max
    }
  }
}
