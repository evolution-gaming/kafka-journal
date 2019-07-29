package com.evolutiongaming.kafka.journal.replicator

import cats.effect.{Resource, Sync}
import cats.implicits._
import com.evolutiongaming.skafka.ClientId
import com.evolutiongaming.skafka.consumer.ConsumerMetrics
import com.evolutiongaming.smetrics.CollectorRegistryPrometheus
import io.prometheus.client.CollectorRegistry

object ReplicatorMetrics {

  def of[F[_] : Sync](registry: CollectorRegistry, clientId: ClientId): Resource[F, Replicator.Metrics[F]] = {
    val replicator = TopicReplicatorMetrics.of[F](registry)
    val journal    = ReplicatedJournalMetrics.of[F](registry)
    for {
      replicator <- Resource.liftF(replicator)
      journal    <- Resource.liftF(journal)
      consumer   <- ConsumerMetrics.of(CollectorRegistryPrometheus(registry))
    } yield {
      Replicator.Metrics[F](journal.some, replicator.some, consumer(clientId).some)
    }
  }
}