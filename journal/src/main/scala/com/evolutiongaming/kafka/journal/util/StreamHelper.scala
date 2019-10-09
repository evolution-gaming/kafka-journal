package com.evolutiongaming.kafka.journal.util

import cats.implicits._
import cats.{ApplicativeError, MonadError, StackSafeMonad}
import com.evolutiongaming.sstream.Stream

// TODO move to sstream
object StreamHelper {

  implicit class StreamOpsStreamHelper[F[_], A](val self: Stream[F, A]) extends AnyVal {

    def handleErrorWith[E](f: E => Stream[F, A])(implicit F: ApplicativeError[F, E]): Stream[F, A] = new Stream[F, A] {

      def foldWhileM[L, R](l: L)(f1: (L, A) => F[Either[L, R]]) = {
        self.foldWhileM(l)(f1).handleErrorWith { a => f(a).foldWhileM(l)(f1) }
      }
    }
  }


  implicit def monadErrorStream[F[_], E](implicit F: MonadError[F, E]): MonadError[Stream[F, *], E] = {

    new MonadError[Stream[F, *], E] with StackSafeMonad[Stream[F, *]] {

      def flatMap[A, B](fa: Stream[F, A])(f: A => Stream[F, B]) = fa.flatMap(f)

      def pure[A](a: A) = Stream.single[F, A](a)

      override def map[A, B](fa: Stream[F, A])(f: A => B) = fa.map(f)

      def raiseError[A](a: E) = Stream.lift(a.raiseError[F, A])

      def handleErrorWith[A](fa: Stream[F, A])(f: E => Stream[F, A]) = fa.handleErrorWith(f)
    }
  }
}
