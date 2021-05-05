package com.evolutiongaming.kafka.journal.util

import cats.data.OptionT
import cats.effect.syntax.all._
import cats.effect.{Concurrent, ExitCase}
import cats.kernel.CommutativeMonoid
import cats.syntax.all._
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

    def orElsePar[B >: A](fa: F[Option[B]])(implicit F: Concurrent[F]): F[Option[B]] = {
      for {
        fiber <- fa.start
        value <- self.guaranteeCase {
          case ExitCase.Completed => ().pure[F]
          case ExitCase.Canceled  => fiber.cancel
          case ExitCase.Error(_)  => fiber.cancel
        }
        value <- value match {
          case Some(value) => fiber.cancel.as(value.some)
          case None        => fiber.join
        }
      } yield value
    }
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
