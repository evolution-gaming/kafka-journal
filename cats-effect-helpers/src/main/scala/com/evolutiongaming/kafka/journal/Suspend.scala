package com.evolutiongaming.kafka.journal

trait Suspend[F[_]] {

  def suspend[A](thunk: => F[A]): F[A]
}

object Suspend {
  def apply[F[_]](implicit F: Suspend[F]): Suspend[F] = F
}
