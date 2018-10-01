package com.evolutiongaming.kafka.journal.replicator

object MetricsHelper {

  implicit class LongOps(val self: Long) extends AnyVal {
    def toSeconds: Double = self.toDouble / 1000
  }
}
