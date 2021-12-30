package com.evolutiongaming.kafka.journal.replicator

import cats.effect.Resource
import cats.syntax.all._
import cats.{Applicative, Defer, ~>}
import com.evolutiongaming.catshelper.CatsHelper._
import com.evolutiongaming.skafka.Topic
import cats.effect.MonadCancel

trait TopicFlowOf[F[_]] {

  def apply(topic: Topic): Resource[F, TopicFlow[F]]
}

object TopicFlowOf {

  def empty[F[_] : Applicative]: TopicFlowOf[F] = (_: Topic) => TopicFlow.empty[F].pure[F].toResource


  implicit class TopicFlowOfOps[F[_]](val self: TopicFlowOf[F]) extends AnyVal {

    def mapK[G[_]](f: F ~> G)(implicit B: MonadCancel[F, Throwable], D: Defer[G], G: Applicative[G]): TopicFlowOf[G] = {
      topic: Topic => self(topic).map(_.mapK(f)).mapK(f)
    }
  }
}
