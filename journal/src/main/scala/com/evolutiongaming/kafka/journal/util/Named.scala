package com.evolutiongaming.kafka.journal.util

// TODO make a Named monad ?
trait Named[F[_]] {

  def apply[A](fa: F[A], name: String): F[A]
}