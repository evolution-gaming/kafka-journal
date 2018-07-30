package com.evolutiongaming.kafka.journal.eventual.cassandra

object Helper {
  implicit class AnyOps[T](val self: T) extends AnyVal {
    def some: Option[T] = Some(self)
  }
}
