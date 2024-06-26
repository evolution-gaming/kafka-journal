package com.evolutiongaming.kafka.journal.util

trait Named[F[_]] {

  def apply[A](fa: F[A], name: String): F[A]
}
