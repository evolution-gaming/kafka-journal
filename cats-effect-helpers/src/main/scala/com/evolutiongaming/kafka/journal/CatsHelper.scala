package com.evolutiongaming.kafka.journal

import cats.effect._
import cats.implicits._
import cats.kernel.CommutativeMonoid
import cats.{Applicative, CommutativeApplicative, Eval, Foldable, MonadError, Monoid, Parallel, UnorderedFoldable, UnorderedTraverse}
import com.evolutiongaming.catshelper.EffectHelper._

import scala.collection.immutable
import scala.concurrent.duration.FiniteDuration

object CatsHelper {

  type MonadErrorE[F[_]] = MonadError[F, Throwable]

  type BracketE[F[_]] = Bracket[F, Throwable]
  

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


  implicit class ParallelIdOps[M[_], F[_]](val self: Parallel[M]) extends AnyVal {

    def commutativeApplicative: CommutativeApplicative[F] = {
      val applicative = self.applicative
      new CommutativeApplicative[F] {

        def pure[A](x: A) = applicative.pure(x)

        def ap[A, B](ff: F[A => B])(fa: F[A]) = applicative.ap(ff)(fa)
      }
    }
  }


  implicit class ParallelOps(val self: Parallel.type) extends AnyVal {

    def foldMap[T[_] : Foldable, M[_], F[_], A, B : Monoid](ta: T[A])(f: A => M[B])(implicit P: Parallel[M, F]): M[B] = {
      implicit val applicative = P.applicative
      implicit val monoid = Applicative.monoid[F, B]
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


  implicit class FOps1[F[_], A](val self: F[A]) extends AnyVal {

    def error[E](implicit bracket: Bracket[F, E]): F[Option[E]] = {
      self.redeem[Option[E], E](_.some, _ => none[E])
    }

    def timeout(duration: FiniteDuration)(implicit F: Concurrent[F], timer: Timer[F]): F[A] = {
      Concurrent.timeout(self, duration)
    }
  }


  implicit class TOps[T[_], A](val self: T[A]) extends AnyVal {
    
    def parFoldMap[M[_], F[_], B : Monoid](f: A => M[B])(implicit F: Foldable[T], P: Parallel[M, F]): M[B] = {
      Parallel.foldMap(self)(f)
    }
  }


  implicit class TMOps[T[_], M[_], A](val self: T[M[A]]) extends AnyVal {

    def parFold[F[_]](implicit F: Foldable[T], P: Parallel[M, F], M : Monoid[A]): M[A] = {
      Parallel.fold(self)
    }
  }


  implicit class ResourceOps[F[_], A](val self: Resource[F, A]) extends AnyVal {

    def start[B](use: A => F[B])(implicit F: Concurrent[F], cs: ContextShift[F]): F[Fiber[F, B]] = {
      StartResource(self)(use)
    }
  }
}
