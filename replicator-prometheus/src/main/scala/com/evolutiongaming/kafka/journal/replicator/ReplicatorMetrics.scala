package com.evolutiongaming.kafka.journal.replicator

import com.evolutiongaming.skafka.ClientId
import com.evolutiongaming.skafka.consumer.PrometheusConsumerMetrics
import io.prometheus.client.CollectorRegistry

object ReplicatorMetrics {

  def apply[F[_]](
    registry: CollectorRegistry,
    clientId: ClientId)(implicit unit: F[Unit]): Replicator.Metrics[F] = {

    val replicator = TopicReplicatorMetrics(registry)
    val journal = ReplicatedJournalMetrics(registry)
    val consumer = PrometheusConsumerMetrics(registry)(clientId)
    Replicator.Metrics(Some(journal), Some(replicator), Some(consumer))
  }
}