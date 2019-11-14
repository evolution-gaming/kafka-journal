package com.evolutiongaming.kafka.journal.replicator

import cats.effect.Resource
import com.evolutiongaming.kafka.journal.replicator.SubscriptionFlow.Consumer
import com.evolutiongaming.skafka.Topic

trait TopicFlowOf[F[_]] {

  def apply(
    topic: Topic,
    consumer: Consumer[F]/*TODO not needed*/
  ): Resource[F, TopicFlow[F]]
}
