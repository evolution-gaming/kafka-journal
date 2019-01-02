package com.evolutiongaming.kafka.journal.util

import cats.effect._
import cats.kernel.CommutativeMonoid
import cats.{Applicative, CommutativeApplicative, Eval, Foldable, Monoid, Parallel, UnorderedFoldable, UnorderedTraverse}

import scala.collection.immutable
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

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

    def commutativeApplicative: CommutativeApplicative[F] = {
      val applicative = self.applicative
      new CommutativeApplicative[F] {

        def pure[A](x: A) = applicative.pure(x)

        def ap[A, B](ff: F[A => B])(fa: F[A]) = applicative.ap(ff)(fa)
      }
    }
  }


  implicit class ParallelOps(val self: Parallel.type) extends AnyVal {

    def foldMap[T[_] : Foldable, M[_], F[_], A, B : Monoid](ta: T[A])(f: A => M[B])(implicit P: Parallel[M, F]/*, monoid: Monoid[F[B]] TODO*/): M[B] = {
      implicit val commutativeApplicative = P.commutativeApplicative // TODO
      implicit val commutativeMonoid = Applicative.monoid[F, B] // TODO
      val fb = Foldable[T].foldMap(ta)(f.andThen(P.parallel.apply))
      P.sequential(fb)
    }

    def fold[T[_] : Foldable, M[_], F[_], A : Monoid](tma: T[M[A]])(implicit P: Parallel[M, F]): M[A] = {
      foldMap(tma)(identity)
    }

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


  implicit class BracketIdOps[F[_], E](val self: Bracket[F, E]) extends AnyVal {

    def redeem[A, B](fa: F[A])(recover: E => B, map: A => B): F[B] = {
      val fb = self.map(fa)(map)
      self.handleError(fb)(recover)
    }

    def redeemWith[A, B](fa: F[A])(recover: E => F[B], flatMap: A => F[B]): F[B] = {
      val fb = self.flatMap(fa)(flatMap)
      self.handleErrorWith(fb)(recover)
    }
  }


  implicit val FoldableIterable: Foldable[Iterable] = new Foldable[Iterable] {

    def foldLeft[A, B](fa: Iterable[A], b: B)(f: (B, A) => B) = {
      fa.foldLeft(b)(f)
    }

    def foldRight[A, B](fa: Iterable[A], lb: Eval[B])(f: (A, Eval[B]) => Eval[B]) = {
      fa.foldRight(lb)(f)
    }
  }

  
  implicit val FoldableImmutableIterable: Foldable[immutable.Iterable] = new Foldable[immutable.Iterable] {

    def foldLeft[A, B](fa: immutable.Iterable[A], b: B)(f: (B, A) => B) = {
      FoldableIterable.foldLeft(fa, b)(f)
    }

    def foldRight[A, B](fa: immutable.Iterable[A], lb: Eval[B])(f: (A, Eval[B]) => Eval[B]) = {
      FoldableIterable.foldRight(fa, lb)(f)
    }
  }


  implicit class FIdOps[F[_], A](val self: F[A]) extends AnyVal {

    def redeem[B, E](recover: E => B)(map: A => B)(implicit bracket: Bracket[F, E]): F[B] = {
      bracket.redeem(self)(recover, map)
    }

    def redeemWith[B, E](recover: E => F[B])(flatMap: A => F[B])(implicit bracket: Bracket[F, E]): F[B] = {
      bracket.redeemWith(self)(recover, flatMap)
    }

    def unsafeToFuture()(implicit toFuture: ToFuture[F]): Future[A] = toFuture(self)

    def timeoutFixed(duration: FiniteDuration)(implicit F: Concurrent[F], timer: Timer[F]): F[A] = {
      TimeoutFixed[F, A](self, duration)
    }
  }


  implicit class ResourceOps[F[_], A](val self: Resource[F, A]) extends AnyVal {

    def start[B](use: A => F[B])(implicit F: Concurrent[F], cs: ContextShift[F]): F[Fiber[F, B]] = {
      StartRes(self)(use)
    }
  }
}
