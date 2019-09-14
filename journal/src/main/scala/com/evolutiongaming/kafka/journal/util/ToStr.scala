package com.evolutiongaming.kafka.journal.util

import cats.Contravariant

trait ToStr[A] {

  def apply(a: A): String
}

object ToStr {

  def apply[A](implicit F: ToStr[A]): ToStr[A] = F


  implicit val contravariantToStr: Contravariant[ToStr] = new Contravariant[ToStr] {

    def contramap[A, B](fa: ToStr[A])(f: B => A) = (b: B) => fa(f(b))
  }


  implicit val throwableToStr: ToStr[Throwable] = _.getMessage


  object implicits {

    implicit class AnyOpsToStr[A](val self: A) extends AnyVal {

      def toStr(implicit toStr: ToStr[A]): String = toStr(self)
    }
  }
}