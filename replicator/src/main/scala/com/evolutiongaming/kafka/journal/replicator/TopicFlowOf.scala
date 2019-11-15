package com.evolutiongaming.kafka.journal.replicator

import cats.effect.Resource
import com.evolutiongaming.skafka.Topic

trait TopicFlowOf[F[_]] {

  def apply(topic: Topic): Resource[F, TopicFlow[F]]
}
