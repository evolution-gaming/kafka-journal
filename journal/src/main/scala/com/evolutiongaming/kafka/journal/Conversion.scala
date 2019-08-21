package com.evolutiongaming.kafka.journal

import cats.implicits._
import cats.{Contravariant, Functor}

trait Conversion[F[_], -A, B] {

  def apply(a: A): F[B]
}


object Conversion {

  def apply[F[_], A, B](implicit F: Conversion[F, A, B]): Conversion[F, A, B] = F


  implicit def functorConversion[F[_] : Functor, C]: Functor[Conversion[F, C, ?]] = new Functor[Conversion[F, C, ?]] {

    def map[A, B](fa: Conversion[F, C, A])(f: A => B) = (a: C) => fa(a).map(f)
  }


  implicit def contravariantConversion[F[_], C]: Contravariant[Conversion[F, ?, C]] = new Contravariant[Conversion[F, ?, C]] {

    def contramap[A, B](fa: Conversion[F, A, C])(f: B => A) = (a: B) => fa(f(a))
  }


  object implicits {

    implicit class ConversionIdOps[A](val self: A) extends AnyVal {

      def convert[F[_], B](implicit F: Conversion[F, A, B]): F[B] = F(self)
    }
  }
}