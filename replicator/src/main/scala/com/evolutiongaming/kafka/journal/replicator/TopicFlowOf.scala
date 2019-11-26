package com.evolutiongaming.kafka.journal.replicator

import cats.effect.{Bracket, Resource}
import cats.implicits._
import cats.{Applicative, Defer, ~>}
import com.evolutiongaming.skafka.Topic

trait TopicFlowOf[F[_]] {

  def apply(topic: Topic): Resource[F, TopicFlow[F]]
}

object TopicFlowOf {

  def empty[F[_] : Applicative]: TopicFlowOf[F] = (_: Topic) => Resource.liftF(TopicFlow.empty[F].pure[F])


  implicit class TopicFlowOfOps[F[_]](val self: TopicFlowOf[F]) extends AnyVal {

    def mapK[G[_]](f: F ~> G)(implicit B: Bracket[F, Throwable], D: Defer[G], G: Applicative[G]): TopicFlowOf[G] = {
      topic: Topic => self(topic).map(_.mapK(f)).mapK(f)
    }
  }
}
