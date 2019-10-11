package com.evolutiongaming.kafka.journal.util

import cats.implicits._
import cats.{Monad, MonadError, StackSafeMonad}
import com.evolutiongaming.sstream.Stream
import com.evolutiongaming.sstream.Stream.StreamOps

object StreamHelper {

  implicit class StreamOpsStreamHelper[F[_], A](val self: Stream[F, A]) extends AnyVal {

    def flatMapLast[B >: A](f: Option[A] => Stream[F, B])(implicit F: Monad[F]): Stream[F, B] = new Stream[F, B] {

      def foldWhileM[L, R](l: L)(f1: (L, B) => F[Either[L, R]]) = {
        self
          .foldWhileM((l, none[A])) { case ((l, _), a) =>
            f1(l, a).map {
              case Left(l)        => (l, a.some).asLeft[R]
              case a: Right[L, R] => a.leftCast[(L, Option[A])]
            }
          }
          .flatMap {
            case Left((l, a))                => f(a).foldWhileM(l)(f1)
            case a: Right[(L, Option[A]), R] => a.leftCast[L].pure[F]
          }
      }
    }
  }


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
