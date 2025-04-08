package com.evolutiongaming.kafka.journal.replicator

import cats.effect.syntax.resource.*
import cats.effect.{MonadCancel, Resource}
import cats.syntax.all.*
import cats.{Applicative, ~>}
import com.evolutiongaming.skafka.Topic

private[journal] trait TopicFlowOf[F[_]] {

  def apply(topic: Topic): Resource[F, TopicFlow[F]]
}

private[journal] object TopicFlowOf {

  def empty[F[_]: Applicative]: TopicFlowOf[F] = (_: Topic) => TopicFlow.empty[F].pure[F].toResource

  implicit class TopicFlowOfOps[F[_]](val self: TopicFlowOf[F]) extends AnyVal {

    def mapK[G[_]](
      f: F ~> G,
    )(implicit
      B: MonadCancel[F, Throwable],
      BG: MonadCancel[G, Throwable],
    ): TopicFlowOf[G] = {
      (topic: Topic) => self(topic).map(_.mapK(f)).mapK(f)
    }
  }
}
