package com.evolutiongaming.kafka.journal

trait FlatMap[F[_]] {

  def flatMap[A, B](fa: F[A], afb: A => F[B]): F[B]

  def map[A, B](fa: F[A], ab: A => B): F[B]
}

object FlatMap {

  implicit class FlatMapOps[A, F[_]](val fa: F[A]) extends AnyVal {

    def map[B](f: A => B)(implicit F: FlatMap[F]): F[B] = F.map(fa, f)

    def flatMap[B](afb: A => F[B])(implicit F: FlatMap[F]): F[B] = F.flatMap(fa, afb)
  }
}