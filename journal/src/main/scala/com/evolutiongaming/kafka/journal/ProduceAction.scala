package com.evolutiongaming.kafka.journal

import cats.syntax.all.*
import cats.{Monad, ~>}
import com.evolutiongaming.kafka.journal.conversions.ActionToProducerRecord

trait ProduceAction[F[_]] {

  def apply(action: Action): F[PartitionOffset]
}

object ProduceAction {

  def apply[F[_]: Monad](
    producer: Journals.Producer[F],
  )(implicit actionToProducerRecord: ActionToProducerRecord[F]): ProduceAction[F] = { (action: Action) =>
    {
      for {
        producerRecord  <- actionToProducerRecord(action)
        partitionOffset <- producer.send(producerRecord)
      } yield partitionOffset
    }
  }

  implicit class ProduceActionOps[F[_]](val self: ProduceAction[F]) extends AnyVal {

    def mapK[G[_]](f: F ~> G): ProduceAction[G] = (action: Action) => f(self(action))
  }
}
