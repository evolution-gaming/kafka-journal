package com.evolutiongaming.kafka.journal.util

import cats.ApplicativeError
import com.evolutiongaming.kafka.journal.util.FromStr.implicits._
import com.evolutiongaming.kafka.journal.util.ToStr.implicits._

// TODO expiry: remove
object ApplicativeString {

  def apply[F[_], E: ToStr : FromStr](implicit F: ApplicativeError[F, E]): ApplicativeString[F] = {

    new ApplicativeString[F] {

      def raiseError[A](a: String) = F.raiseError(a.fromStr[E])

      def handleErrorWith[A](fa: F[A])(f: String => F[A]) = F.handleErrorWith(fa)(e => f(e.toStr))

      def pure[A](a: A) = F.pure(a)

      def ap[A, B](ff: F[A => B])(fa: F[A]) = F.ap(ff)(fa)
    }
  }
}

