package com.evolutiongaming.kafka.journal

import com.evolutiongaming.kafka.journal.FoldWhileHelper.Switch

trait IO[F[_]] extends FlatMap[F] {

  def pure[A](a: A): F[A]

  def point[A](a: => A): F[A]

  def unit[A]: F[Unit]

  def unit[A](fa: F[A]): F[Unit]

  def fold[A, S](iter: Iterable[A], s: S)(f: (S, A) => F[S]): F[S]

  def foldUnit[A](iter: Iterable[F[A]]): F[Unit] = fold(iter, ()) { (_, a) => unit(a) }

  def foldWhile[S](s: S)(f: S => F[Switch[S]]): F[S]

  def catchAll[A, B >: A](fa: F[A], f: Throwable => F[B]): F[B]
}

object IO {
  def apply[F[_]](implicit F: IO[F]): IO[F] = F
}