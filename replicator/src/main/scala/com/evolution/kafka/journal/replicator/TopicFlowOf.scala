package com.evolution.kafka.journal.replicator

import cats.Applicative
import cats.effect.Resource
import cats.effect.syntax.resource.*
import cats.syntax.all.*
import com.evolutiongaming.skafka.Topic

private[journal] trait TopicFlowOf[F[_]] {

  def apply(topic: Topic, commit: TopicCommit[F]): Resource[F, TopicFlow[F]]
}

private[journal] object TopicFlowOf {

  def empty[F[_]: Applicative]: TopicFlowOf[F] = (_: Topic, _: TopicCommit[F]) => TopicFlow.empty[F].pure[F].toResource
}
