package com.evolutiongaming.kafka.journal.util

import cats.effect._
import cats.implicits._
import cats.kernel.CommutativeMonoid
import cats.{Applicative, CommutativeApplicative}
import com.evolutiongaming.catshelper.CatsHelper._

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


  implicit class FOps1[F[_], A](val self: F[A]) extends AnyVal {

    def error[E](implicit bracket: Bracket[F, E]): F[Option[E]] = {
      self.redeem[Option[E], E](_.some, _ => none[E])
    }
  }

  
  implicit class ResourceOps[F[_], A](val self: Resource[F, A]) extends AnyVal {

    def start[B](use: A => F[B])(implicit F: Concurrent[F], cs: ContextShift[F]): F[Fiber[F, B]] = {
      StartResource(self)(use)
    }
  }
}
