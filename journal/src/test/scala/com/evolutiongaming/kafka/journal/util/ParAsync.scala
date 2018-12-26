package com.evolutiongaming.kafka.journal.util

import cats.kernel.CommutativeMonoid
import cats.{Traverse, UnorderedFoldable, UnorderedTraverse}
import com.evolutiongaming.concurrent.async.Async
import com.evolutiongaming.concurrent.async.AsyncConverters._

object ParAsync extends Par[Async] {

  def sequence[T[_] : Traverse, A](tfa: T[Async[A]]) = {
    Traverse[T].map(tfa)(_.get()).async
  }

  def unorderedSequence[T[_] : UnorderedTraverse, A](tfa: T[Async[A]]) = {
    UnorderedTraverse[T].unorderedTraverse[cats.Id, Async[A], A](tfa)(_.get()).async
  }

  def unorderedFold[T[_] : UnorderedFoldable, A: CommutativeMonoid](tfa: T[Async[A]]) = {
    unorderedFoldMap(tfa)(identity)
  }

  def unorderedFoldMap[T[_] : UnorderedFoldable, A, B: CommutativeMonoid](ta: T[A])(f: A => Async[B]) = {
    UnorderedFoldable[T].unorderedFoldMap(ta)(f.andThen(_.get())).async
  }

  def mapN[Z, A0, A1, A2](t3: (Async[A0], Async[A1], Async[A2]))(f: (A0, A1, A2) => Z) = {
    val (t0, t1, t2) = t3
    f(t0.get(), t1.get(), t2.get()).async
  }

  def mapN[Z, A0, A1, A2, A3, A4, A5, A6, A7, A8, A9](t10: (Async[A0], Async[A1], Async[A2], Async[A3], Async[A4], Async[A5], Async[A6], Async[A7], Async[A8], Async[A9]))(f: (A0, A1, A2, A3, A4, A5, A6, A7, A8, A9) => Z) = {
    val (t0, t1, t2, t3, t4, t5, t6, t7, t8, t9) = t10
    f(t0.get(), t1.get(), t2.get(), t3.get(), t4.get(), t5.get(), t6.get(), t7.get(), t8.get(), t9.get()).async
  }

  def tupleN[A0, A1](f0: Async[A0], f1: Async[A1]) = {
    for {
      a0 <- f0
      a1 <- f1
    } yield (a0, a1)
  }
}
