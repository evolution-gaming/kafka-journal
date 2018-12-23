package com.evolutiongaming.kafka.journal.util

import cats.implicits._
import cats.kernel.CommutativeMonoid
import cats.{Applicative, Apply, CommutativeApplicative, FlatMap, Parallel, UnorderedFoldable}

import scala.collection.immutable

object CatsHelper {

  implicit class FOps[F[_], A](val self: F[A]) extends AnyVal {

    def unit(implicit flatMap: FlatMap[F]): F[Unit] = {
      for {_ <- self} yield {}
    }
  }


  implicit def commutativeMonoidF[F[_] : CommutativeApplicative, A: CommutativeMonoid]: CommutativeMonoid[F[A]] = {
    new CommutativeMonoid[F[A]] {
      def empty = {
        Applicative[F].pure(CommutativeMonoid[A].empty)
      }

      def combine(x: F[A], y: F[A]) = {
        Apply[F].map2(x, y)(CommutativeMonoid[A].combine)
      }
    }
  }


  implicit class CommutativeApplicativeOps(val self: CommutativeApplicative.type) extends AnyVal {

    def commutativeMonoid[F[_] : Applicative/*TODO CommutativeApplicative*/, A: CommutativeMonoid]: CommutativeMonoid[F[A]] = {
      new CommutativeMonoid[F[A]] {
        def empty = {
          Applicative[F].pure(CommutativeMonoid[A].empty)
        }

        def combine(x: F[A], y: F[A]) = {
          Applicative[F].map2(x, y)(CommutativeMonoid[A].combine)
        }
      }
    }
  }


  implicit class ParallelOps(val self: Parallel.type) extends AnyVal {

    def unorderedFoldMap[T[_] : UnorderedFoldable, M[_], F[_], A, B: CommutativeMonoid](ta: T[A])(f: A => M[B])(implicit P: Parallel[M, F]): M[B] = {
      implicit val applicative = P.applicative
      implicit val commutativeMonoid = CommutativeApplicative.commutativeMonoid[F, B]
      val fb = UnorderedFoldable[T].unorderedFoldMap(ta)(f.andThen(P.parallel.apply))
      P.sequential(fb)
    }

    def unorderedFold[T[_] : UnorderedFoldable, M[_], F[_], A: CommutativeMonoid](ta: T[M[A]])(implicit P: Parallel[M, F]): M[A] = {
      unorderedFoldMap(ta)(identity)
    }
  }


  implicit val FoldableIterable: UnorderedFoldable[Iterable] = new UnorderedFoldable[Iterable] {

    def unorderedFoldMap[A, B](fa: Iterable[A])(f: A => B)(implicit F: CommutativeMonoid[B]) = {
      fa.foldLeft(F.empty)((b, a) => F.combine(b, f(a)))
    }
  }


  implicit val FoldableImmutableIterable: UnorderedFoldable[immutable.Iterable] = new UnorderedFoldable[immutable.Iterable] {

    def unorderedFoldMap[A, B](fa: immutable.Iterable[A])(f: A => B)(implicit F: CommutativeMonoid[B]) = {
      fa.foldLeft(F.empty)((b, a) => F.combine(b, f(a)))
    }
  }
}
