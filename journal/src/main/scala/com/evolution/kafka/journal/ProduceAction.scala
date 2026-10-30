package com.evolution.kafka.journal

import cats.syntax.all.*
import cats.{Monad, ~>}
import com.evolution.kafka.journal.conversions.ActionToProducerRecord

trait ProduceAction[F[_]] {

  def apply(action: Action): F[PartitionOffset]
}

object ProduceAction {

  def apply[F[_]: Monad](
    producer: Journals.Producer[F],
  )(implicit
    actionToProducerRecord: ActionToProducerRecord[F],
  ): ProduceAction[F] = new ProduceAction[F] {

    def apply(action: Action): F[PartitionOffset] = {
      for {
        producerRecord <- actionToProducerRecord(action)
        partitionOffset <- producer.send(producerRecord)
      } yield partitionOffset
    }
  }

  implicit class ProduceActionOps[F[_]](val self: ProduceAction[F]) extends AnyVal {

    def mapK[G[_]](f: F ~> G): ProduceAction[G] = new ProduceAction[G] {

      def apply(action: Action): G[PartitionOffset] = f(self(action))
    }
  }
}
