package com.evolutiongaming.kafka.journal

import scala.concurrent.Future

trait IO[F[_]] {

  def pure[A](a: A): F[A]

  def point[A](a: => A): F[A]

  def unit[A]: F[Unit]

  def unit[A](fa: F[A]): F[Unit]

  def sync[A](a: => A): F[A]

  def fold[A, S](iter: Iterable[A], s: S)(f: (S, A) => F[S]): F[S]

  def foldUnit[A](iter: Iterable[F[A]]): F[Unit] = fold(iter, ()) { (_, a) => unit(a) }

  def foldWhile[S](s: S)(f: S => F[FoldWhile.Switch[S]]): F[S]

  def flatMapFailure[A, B >: A](fa: F[A], f: Throwable => F[B]): F[B]

  def flatMap[A, B](fa: F[A], afb: A => F[B]): F[B]

  def map[A, B](fa: F[A], ab: A => B): F[B]
}

object IO {
  def apply[F[_]](implicit F: IO[F]): IO[F] = F


  object syntax {

    implicit class IOOps[A, F[_]](fa: F[A]) {

      def map[B](f: A => B)(implicit F: IO[F]): F[B] = F.map(fa, f)

      def flatMap[B](afb: A => F[B])(implicit F: IO[F]): F[B] = F.flatMap(fa, afb)

      def catchAll[B >: A](ftb: Throwable => F[B])(implicit F: IO[F]): F[B] = F.flatMapFailure(fa, ftb)
    }

    implicit class IOIdOps[A](val self: A) extends AnyVal {
      def pure[F[_] : IO]: F[A] = IO[F].pure(self)
    }

    implicit class FutureIdOps[A](val self: () => Future[A]) extends AnyVal {
      def toIO[F[_] : FromFuture]: F[A] = FromFuture[F].apply(self)
    }


    implicit class OptionIdOps[A](val self: A) extends AnyVal {
      def some: Option[A] = Some(self)
    }

    def none[A]: Option[A] = Option.empty

    def unit[F[_] : IO]: F[Unit] = IO[F].unit
  }
}