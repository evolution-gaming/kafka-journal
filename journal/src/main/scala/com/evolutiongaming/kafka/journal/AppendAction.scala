package com.evolutiongaming.kafka.journal

import cats.implicits._
import cats.~>
import com.evolutiongaming.catshelper.MonadThrowable
import com.evolutiongaming.kafka.journal.conversions.ActionToProducerRecord

trait AppendAction[F[_]] {
  
  def apply(action: Action): F[PartitionOffset]
}

object AppendAction {

  def apply[F[_] : MonadThrowable](
    producer: Journal.Producer[F])(implicit
    actionToProducerRecord: ActionToProducerRecord[F]
  ): AppendAction[F] = {
    action: Action => {
      val partitionOffset = for {
        producerRecord  <- actionToProducerRecord(action)
        partitionOffset <- producer.send(producerRecord)
      } yield partitionOffset
      partitionOffset.handleErrorWith { cause =>
        val error = JournalError(s"failed to append $action", cause.some)
        error.raiseError[F, PartitionOffset]
      }
    }
  }


  implicit class AppendActionOps[F[_]](val self: AppendAction[F]) extends AnyVal {

    def mapK[G[_]](f: F ~> G): AppendAction[G] = new AppendAction[G] {
      def apply(action: Action) = f(self(action))
    }
  }
}