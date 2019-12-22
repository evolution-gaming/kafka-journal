package com.evolutiongaming.kafka.journal.util

import cats.Functor

// TODO expiry: remove
trait FromStr[A] {

  def apply(a: String): A
}

object FromStr {

  def apply[A](implicit F: FromStr[A]): FromStr[A] = F


  implicit val functorFromStr: Functor[FromStr] = new Functor[FromStr] {
    def map[A, B](fa: FromStr[A])(f: A => B) = (a: String) => f(fa(a))
  }


  object implicits {

    implicit class StringOpsFromStr(val self: String) extends AnyVal {

      def fromStr[A](implicit fromStr: FromStr[A]): A = fromStr(self)
    }
  }
}