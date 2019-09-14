package com.evolutiongaming.kafka.journal.util

import cats.MonadError
import com.evolutiongaming.kafka.journal.util.FromStr.implicits._
import com.evolutiongaming.kafka.journal.util.ToStr.implicits._

object MonadString {

  def apply[F[_], E: ToStr : FromStr](implicit F: MonadError[F, E]): MonadString[F] = {

    new MonadString[F] {

      def raiseError[A](a: String) = F.raiseError(a.fromStr[E])

      def handleErrorWith[A](fa: F[A])(f: String => F[A]) = F.handleErrorWith(fa)(e => f(e.toStr))

      def pure[A](a: A) = F.pure(a)

      def flatMap[A, B](fa: F[A])(f: A => F[B]) = F.flatMap(fa)(f)

      def tailRecM[A, B](a: A)(f: A => F[Either[A, B]]) = F.tailRecM(a)(f)
    }
  }
}
