package com.evolutiongaming.kafka.journal.replicator

import io.prometheus.client.CollectorRegistry

object ReplicatorMetrics {

  def apply[F[_]](
    registry: CollectorRegistry,
    prefix: String = "replicator")(implicit unit: F[Unit]): Replicator.Metrics[F] = {
    val replicator = TopicReplicatorMetrics(registry, prefix)
    val journal = ReplicatedJournalMetrics(registry, s"${ prefix }_journal")
    Replicator.Metrics(journal, replicator)
  }
}
