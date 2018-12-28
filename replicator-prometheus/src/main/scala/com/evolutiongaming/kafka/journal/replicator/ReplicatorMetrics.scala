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
      consumer   <- Sync[F].delay { PrometheusConsumerMetrics(registry)(clientId) } // TODO
    } yield {
      Replicator.Metrics[F](Some(journal), Some(replicator), Some(consumer))
    }
  }
}