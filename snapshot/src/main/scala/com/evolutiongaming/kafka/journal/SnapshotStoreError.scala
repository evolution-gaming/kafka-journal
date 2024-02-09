package com.evolutiongaming.kafka.journal

import cats.syntax.all._

final case class SnapshotStoreError(msg: String, cause: Option[Throwable] = None)
    extends RuntimeException(msg, cause.orNull)

object SnapshotStoreError {

  def apply(msg: String, cause: Throwable): SnapshotStoreError =
    SnapshotStoreError(msg, cause.some)

}
