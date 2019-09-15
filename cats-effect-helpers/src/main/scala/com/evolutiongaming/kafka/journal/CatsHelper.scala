package com.evolutiongaming.kafka.journal

import cats.effect._
import cats.implicits._
import cats.kernel.CommutativeMonoid
import cats.{Applicative, CommutativeApplicative, Eval, Foldable}
import com.evolutiongaming.catshelper.EffectHelper._

import scala.collection.immutable
import scala.concurrent.duration.FiniteDuration

object CatsHelper {

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


  implicit val foldableIterable: Foldable[Iterable] = new Foldable[Iterable] {

    def foldLeft[A, B](fa: Iterable[A], b: B)(f: (B, A) => B) = {
      fa.foldLeft(b)(f)
    }

    def foldRight[A, B](fa: Iterable[A], lb: Eval[B])(f: (A, Eval[B]) => Eval[B]) = {
      fa.foldRight(lb)(f)
    }
  }


  implicit val foldableImmutableIterable: Foldable[immutable.Iterable] = new Foldable[immutable.Iterable] {

    def foldLeft[A, B](fa: immutable.Iterable[A], b: B)(f: (B, A) => B) = {
      foldableIterable.foldLeft(fa, b)(f)
    }

    def foldRight[A, B](fa: immutable.Iterable[A], lb: Eval[B])(f: (A, Eval[B]) => Eval[B]) = {
      foldableIterable.foldRight(fa, lb)(f)
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

  
  implicit class ResourceOps[F[_], A](val self: Resource[F, A]) extends AnyVal {

    def start[B](use: A => F[B])(implicit F: Concurrent[F], cs: ContextShift[F]): F[Fiber[F, B]] = {
      StartResource(self)(use)
    }
  }
}
