package com.evolutiongaming.kafka.journal.util

import cats.data.OptionT
import cats.effect._
import cats.implicits._
import cats.kernel.CommutativeMonoid
import cats.{Applicative, CommutativeApplicative}
import com.evolutiongaming.catshelper.CatsHelper._
import com.evolutiongaming.kafka.journal.util.Fail.implicits._

import scala.concurrent.ExecutionContext

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

    def error[E](implicit bracket: Bracket[F, E]): F[Option[E]] = {
      self.redeem[Option[E], E](_.some, _ => none[E])
    }
  }


  implicit class FOptionOpsCatsHelper[F[_], A](val self: F[Option[A]]) extends AnyVal {
    
    def toOptionT: OptionT[F, A] = OptionT(self)
  }


  implicit class ResourceOpsCatsHelper[F[_], A](val self: Resource[F, A]) extends AnyVal {

    def start[B](use: A => F[B])(implicit F: Concurrent[F]): F[Fiber[F, B]] = {
      StartResource(self)(use)
    }
  }


  implicit class ContextShiftObjCatsHelpers(val self: ContextShift.type) extends AnyVal {

    def empty[F[_] : Applicative]: ContextShift[F] = new ContextShift[F] {

      val shift = ().pure[F]

      def evalOn[A](ec: ExecutionContext)(fa: F[A]) = fa
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
