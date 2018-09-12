package com.evolutiongaming.kafka.journal.replicator

import java.time.Instant

object InstantHelper {

  implicit class InstantOps(val self: Instant) extends AnyVal {

    def -(instant: Instant): Long = {
      self.toEpochMilli - instant.toEpochMilli
    }
  }
}
