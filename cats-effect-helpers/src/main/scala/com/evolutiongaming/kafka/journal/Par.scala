package com.evolutiongaming.kafka.journal

import cats.implicits._
import cats.{Foldable, Monoid, Parallel, Traverse, Applicative}
import com.evolutiongaming.kafka.journal.CatsHelper.ParallelOps

trait Par[F[_]] {

  def sequence[T[_] : Traverse, A](tfa: T[F[A]]): F[T[A]]

  def fold[T[_] : Foldable, A : Monoid](tfa: T[F[A]]): F[A]

  def foldMap[T[_] : Foldable, A, B : Monoid](ta: T[A])(f: A => F[B]): F[B]

  def mapN[Z, A0, A1, A2](
    t3: (F[A0], F[A1], F[A2]))
    (f: (A0, A1, A2) => Z): F[Z]

  def mapN[Z, A0, A1, A2, A3, A4, A5, A6, A7, A8, A9](
    t10: (F[A0], F[A1], F[A2], F[A3], F[A4], F[A5], F[A6], F[A7], F[A8], F[A9]))
    (f: (A0, A1, A2, A3, A4, A5, A6, A7, A8, A9) => Z): F[Z]

  def tupleN[A0, A1](f0: F[A0], f1: F[A1]): F[(A0, A1)]
}

object Par {

  def apply[F[_]](implicit F: Par[F]): Par[F] = F


  implicit def lift[F[_], M[_]](implicit parallel: Parallel[F, M]): Par[F] = new Par[F] {

    def sequence[T[_] : Traverse, A](tfa: T[F[A]]) = {
      Parallel.parSequence(tfa)
    }

    def fold[T[_] : Foldable, A : Monoid](tfa: T[F[A]]) = {
      Parallel.fold(tfa)
    }

    def foldMap[T[_] : Foldable, A, B : Monoid](ta: T[A])(f: A => F[B]) = {
      Parallel.foldMap(ta)(f)
    }

    def mapN[Z, A0, A1, A2](
      t3: (F[A0], F[A1], F[A2]))
      (f: (A0, A1, A2) => Z) = {

      t3.parMapN(f)
    }

    def mapN[Z, A0, A1, A2, A3, A4, A5, A6, A7, A8, A9](
      t10: (F[A0], F[A1], F[A2], F[A3], F[A4], F[A5], F[A6], F[A7], F[A8], F[A9]))(
      f: (A0, A1, A2, A3, A4, A5, A6, A7, A8, A9) => Z) = {

      t10.parMapN(f)
    }

    def tupleN[A0, A1](f0: F[A0], f1: F[A1]) = {
      Parallel.parTuple2(f0, f1)
    }
  }


  def sequential[F[_]](implicit F: Applicative[F]): Par[F] = new Par[F] {

    def sequence[T[_] : Traverse, A](tfa: T[F[A]]) = Traverse[T].sequence(tfa)

    def fold[T[_] : Foldable, A: Monoid](tfa: T[F[A]]) = {
      implicit val monoid = Applicative.monoid[F, A]
      Foldable[T].fold(tfa)
    }

    def foldMap[T[_] : Foldable, A, B: Monoid](ta: T[A])(f: A => F[B]) = {
      implicit val monoid = Applicative.monoid[F, B]
      Foldable[T].foldMap(ta)(f)
    }

    def mapN[Z, A0, A1, A2](t3: (F[A0], F[A1], F[A2]))(f: (A0, A1, A2) => Z) = t3.mapN(f)

    def mapN[Z, A0, A1, A2, A3, A4, A5, A6, A7, A8, A9](
      t10: (F[A0], F[A1], F[A2], F[A3], F[A4], F[A5], F[A6], F[A7], F[A8], F[A9]))(
      f: (A0, A1, A2, A3, A4, A5, A6, A7, A8, A9) => Z) = {

      t10.mapN(f)
    }

    def tupleN[A0, A1](f0: F[A0], f1: F[A1]) = (f0, f1).tupled
  }
}