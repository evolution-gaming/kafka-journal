package com.evolutiongaming.kafka.journal

import com.evolutiongaming.kafka.journal.stream.Stream

trait Settings[F[_]] {
  import Setting.{Key, Value}

  def get(key: Key): F[Option[Setting]]

  def set(key: Key, value: Value): F[Option[Setting]]

  def remove(key: Value): F[Option[Setting]]

  def all: Stream[F, Setting]
}
