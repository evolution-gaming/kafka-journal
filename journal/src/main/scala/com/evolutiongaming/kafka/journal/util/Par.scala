package com.evolutiongaming.kafka.journal.util

import cats.effect.IO
import cats.{Parallel, Traverse}

trait Par[F[_]] {

  def sequence[T[_] : Traverse, A](tfa: T[F[A]]): F[T[A]]

  def traverse[T[_] : Traverse, A, B](ta: T[A])(f: A => F[B]): F[T[B]]
}

object Par {
  
  def apply[F[_]](implicit F: Par[F]): Par[F] = F


  implicit def parIO(implicit parallel: Parallel[IO, IO.Par]): Par[IO] = new Par[IO] {

    def sequence[T[_] : Traverse, A](tfa: T[IO[A]]) = {
      Parallel.parSequence(tfa)
    }

    def traverse[T[_] : Traverse, A, B](ta: T[A])(f: A => IO[B]) = {
      Parallel.parTraverse(ta)(f)
    }
  }
}