package com.evolutiongaming.kafka.journal.replicator

import cats.effect.Sync
import cats.implicits._
import com.evolutiongaming.skafka.ClientId
import com.evolutiongaming.skafka.consumer.PrometheusConsumerMetrics
import io.prometheus.client.CollectorRegistry

object ReplicatorMetrics {

  def of[F[_] : Sync](registry: CollectorRegistry, clientId: ClientId): F[Replicator.Metrics[F]] = {
    for {
      replicator <- TopicReplicatorMetrics.of[F](registry)
      journal    <- ReplicatedJournalMetrics.of[F](registry)
      consumer   <- PrometheusConsumerMetrics.of[F](registry)
    } yield {
      Replicator.Metrics[F](journal.some, replicator.some, consumer(clientId).some)
    }
  }
}