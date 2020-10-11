package com.evolutiongaming.kafka.journal.util

import cats.data.OptionT
import cats.implicits._
import cats.kernel.CommutativeMonoid
import cats.{Applicative, ApplicativeError, CommutativeApplicative}
import com.evolutiongaming.kafka.journal.util.Fail.implicits._


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


  implicit class FOpsCatsHelper[F[_], A](val self: F[A]) extends AnyVal {

    def error[E](implicit F: ApplicativeError[F, E]): F[Option[E]] = {
      self.redeem[Option[E]](_.some, _ => none[E])
    }
  }


  implicit class FOptionOpsCatsHelper[F[_], A](val self: F[Option[A]]) extends AnyVal {
    
    def toOptionT: OptionT[F, A] = OptionT(self)
  }


  implicit class OptionOpsCatsHelper[A](val self: Option[A]) extends AnyVal {

    def getOrError[F[_]: Applicative : Fail](name: => String): F[A] = {
      self.fold {
        s"$name is not defined".fail[F, A]
      } { a =>
        a.pure[F]
      }
    }
  }
}
