package com.evolutiongaming.kafka.journal.replicator

import cats.effect.Resource

private[journal] trait Cache[F[_], K, V] {

  def getOrUpdate(key: K)(value: => Resource[F, V]): F[V]

  def remove(key: K): F[Unit]
}
