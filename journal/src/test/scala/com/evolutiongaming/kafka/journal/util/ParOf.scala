package com.evolutiongaming.kafka.journal.util

import cats.{Foldable, Id, Monoid, Traverse}

object ParOf {

  def id: Par[Id] = new Par[Id] {

    def sequence[T[_] : Traverse, A](tfa: T[Id[A]]) = {
      Traverse[T].map(tfa)(identity)
    }

    def fold[T[_] : Foldable, A: Monoid](tfa: T[Id[A]]) = {
      foldMap(tfa)(identity)
    }

    def foldMap[T[_] : Foldable, A, B: Monoid](ta: T[A])(f: A => Id[B]) = {
      Foldable[T].foldMap(ta)(f)
    }

    def mapN[Z, A0, A1, A2](t3: (Id[A0], Id[A1], Id[A2]))(f: (A0, A1, A2) => Z) = {
      val (t0, t1, t2) = t3
      f(t0, t1, t2)
    }

    def mapN[Z, A0, A1, A2, A3, A4, A5, A6, A7, A8, A9](
      t10: (Id[A0], Id[A1], Id[A2], Id[A3], Id[A4], Id[A5], Id[A6], Id[A7], Id[A8], Id[A9]))(
      f: (A0, A1, A2, A3, A4, A5, A6, A7, A8, A9) => Z) = {

      val (t0, t1, t2, t3, t4, t5, t6, t7, t8, t9) = t10
      f(t0, t1, t2, t3, t4, t5, t6, t7, t8, t9)
    }

    def tupleN[A0, A1](f0: Id[A0], f1: Id[A1]): (A0, A1) = {
      (f0, f1)
    }
  }
}
