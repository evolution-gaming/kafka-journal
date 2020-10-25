package com.evolutiongaming.kafka.journal.util

import cats.syntax.all._
import cats.{MonadError, StackSafeMonad}
import com.evolutiongaming.sstream.Stream
import com.evolutiongaming.sstream.Stream.StreamOps

object StreamHelper {

  implicit def monadErrorStream[F[_], E](implicit F: MonadError[F, E]): MonadError[Stream[F, *], E] = {

    new MonadError[Stream[F, *], E] with StackSafeMonad[Stream[F, *]] {

      def flatMap[A, B](fa: Stream[F, A])(f: A => Stream[F, B]) =   StreamOps(fa).flatMap(f)

      def pure[A](a: A) = Stream.single[F, A](a)

      override def map[A, B](fa: Stream[F, A])(f: A => B) = StreamOps(fa).map(f)

      def raiseError[A](a: E) = Stream.lift(a.raiseError[F, A])

      def handleErrorWith[A](fa: Stream[F, A])(f: E => Stream[F, A]) = StreamOps(fa).handleErrorWith(f)
    }
  }
}
