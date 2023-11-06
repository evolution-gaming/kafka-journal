package com.evolutiongaming.kafka.journal

import java.time.Instant

final case class SnapshotRecord[A](
  snapshot: Snapshot[A],
  timestamp: Instant,
  origin: Option[Origin],
  version: Option[Version]
)
