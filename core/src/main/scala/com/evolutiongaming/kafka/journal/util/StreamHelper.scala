package com.evolutiongaming.kafka.journal.util

import cats.effect.{MonadCancel, Resource}
import cats.syntax.all._
import cats.{FlatMap, Monad, MonadError, StackSafeMonad}
import com.evolutiongaming.sstream.{FoldWhile, Stream}
import com.evolutiongaming.sstream.Stream.StreamOps

object StreamHelper {

  implicit def monadErrorStream[F[_], E](implicit F: MonadError[F, E]): MonadError[Stream[F, *], E] = {

    new MonadError[Stream[F, *], E] with StackSafeMonad[Stream[F, *]] {

      def flatMap[A, B](fa: Stream[F, A])(f: A => Stream[F, B]) =   StreamOps(fa).flatMap(f)

      def pure[A](a: A) = Stream.single[F, A](a)

      override def map[A, B](fa: Stream[F, A])(f: A => B) = StreamOps(fa).map(f)

      def raiseError[A](a: E) = a.raiseError[F, A].toStream

      def handleErrorWith[A](fa: Stream[F, A])(f: E => Stream[F, A]) = StreamOps(fa).handleErrorWith(f)
    }
  }

  implicit class OpsStreamHelper[F[_], A](val self: F[A]) extends AnyVal {
    def toStream1[M[_]](implicit M: Monad[M], G: FoldWhile[F]): Stream[M, A] = Stream[M].apply(self)

    def toStream(implicit F: FlatMap[F]): Stream[F, A] = Stream.lift { self }
  }

  implicit class ResourceOpsStreamHelper[F[_], A](val self: Resource[F, A]) extends AnyVal {
    def toStream(implicit F: MonadCancel[F, Throwable]): Stream[F, A] = Stream.fromResource(self)
  }
}
