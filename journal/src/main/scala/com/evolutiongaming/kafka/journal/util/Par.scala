package com.evolutiongaming.kafka.journal.util

import cats.effect.IO
import cats.implicits._
import cats.kernel.CommutativeMonoid
import cats.{Parallel, Traverse, UnorderedFoldable}
import com.evolutiongaming.kafka.journal.util.CatsHelper._

trait Par[F[_]] {

  def sequence[T[_] : Traverse, A](tfa: T[F[A]]): F[T[A]]

  def unorderedFold[T[_] : UnorderedFoldable, A : CommutativeMonoid](tfa: T[F[A]]): F[A]

  def unorderedFoldMap[T[_] : UnorderedFoldable, A, B : CommutativeMonoid](ta: T[A])(f: A => F[B]): F[B]

  def mapN[Z, A0, A1, A2, A3, A4, A5, A6, A7, A8, A9](
    t10: (F[A0], F[A1], F[A2], F[A3], F[A4], F[A5], F[A6], F[A7], F[A8], F[A9]))
    (f: (A0, A1, A2, A3, A4, A5, A6, A7, A8, A9) => Z): F[Z]
}

object Par {

  def apply[F[_]](implicit F: Par[F]): Par[F] = F

  def io(implicit parallel: Parallel[IO, IO.Par]): Par[IO] = new Par[IO] {

    def sequence[T[_] : Traverse, A](tfa: T[IO[A]]) = {
      Parallel.parSequence(tfa)
    }

    def unorderedFold[T[_] : UnorderedFoldable, A : CommutativeMonoid](tfa: T[IO[A]]) = {
      Parallel.unorderedFold(tfa)
    }

    def unorderedFoldMap[T[_] : UnorderedFoldable, A, B : CommutativeMonoid](ta: T[A])(f: A => IO[B]) = {
      Parallel.unorderedFoldMap(ta)(f)
    }

    def mapN[Z, A0, A1, A2, A3, A4, A5, A6, A7, A8, A9](
      t10: (IO[A0], IO[A1], IO[A2], IO[A3], IO[A4], IO[A5], IO[A6], IO[A7], IO[A8], IO[A9]))(
      f: (A0, A1, A2, A3, A4, A5, A6, A7, A8, A9) => Z) = {

      t10.parMapN(f)
    }
  }

  implicit def ioPar(implicit parallel: Parallel[IO, IO.Par]): Par[IO] = io
}