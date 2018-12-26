package com.evolutiongaming.kafka.journal.util

import cats.kernel.CommutativeMonoid
import cats.{Applicative, CommutativeApplicative, Parallel, UnorderedFoldable, UnorderedTraverse}

import scala.collection.immutable

object CatsHelper {

  /*implicit def commutativeMonoidF[F[_] : CommutativeApplicative, A: CommutativeMonoid]: CommutativeMonoid[F[A]] = {
    new CommutativeMonoid[F[A]] {
      def empty = {
        Applicative[F].pure(CommutativeMonoid[A].empty)
      }

      def combine(x: F[A], y: F[A]) = {
        Apply[F].map2(x, y)(CommutativeMonoid[A].combine)
      }
    }
  }*/


  implicit class CommutativeApplicativeOps(val self: CommutativeApplicative.type) extends AnyVal {

    def commutativeMonoid[F[_] : CommutativeApplicative, A: CommutativeMonoid]: CommutativeMonoid[F[A]] = {
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


  implicit class ParallelIdOps[M[_], F[_]](val self: Parallel[M, F]) extends AnyVal {
    def commutativeApplicative: CommutativeApplicative[F] = CommutativeApplicativeOf(self.applicative)
  }


  implicit class ParallelOps(val self: Parallel.type) extends AnyVal {

    def unorderedFoldMap[T[_] : UnorderedFoldable, M[_], F[_], A, B: CommutativeMonoid](ta: T[A])(f: A => M[B])(implicit P: Parallel[M, F]): M[B] = {
      implicit val commutativeApplicative = P.commutativeApplicative
      implicit val commutativeMonoid = CommutativeApplicative.commutativeMonoid[F, B]
      val fb = UnorderedFoldable[T].unorderedFoldMap(ta)(f.andThen(P.parallel.apply))
      P.sequential(fb)
    }

    def unorderedFold[T[_] : UnorderedFoldable, M[_], F[_], A: CommutativeMonoid](tma: T[M[A]])(implicit P: Parallel[M, F]): M[A] = {
      unorderedFoldMap(tma)(identity)
    }

    def unorderedSequence[T[_]: UnorderedTraverse, M[_], F[_], A](tma: T[M[A]])(implicit P: Parallel[M, F]): M[T[A]] = {
      implicit val commutativeApplicative = P.commutativeApplicative
      val fta: F[T[A]] = UnorderedTraverse[T].unorderedTraverse(tma)(P.parallel.apply)
      P.sequential(fta)
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
