package com.evolutiongaming.kafka.journal

import cats.effect.Sync
import cats.implicits._
import cats.~>
import com.evolutiongaming.kafka.journal.KafkaConverters._
import com.evolutiongaming.skafka.Offset

trait AppendAction[F[_]] {
  def apply(action: Action): F[PartitionOffset]
}

object AppendAction {

  def apply[F[_] : Sync](producer: Journal.Producer[F]): AppendAction[F] = {

    new AppendAction[F] {

      def apply(action: Action) = {
        val producerRecord = action.toProducerRecord
        for {
          metadata  <- producer.send(producerRecord)
          partition  = metadata.topicPartition.partition
          offset    <- metadata.offset.fold {
            // TODO replace this with proper config
            val error = JournalException(action.key, "metadata.offset is missing, make sure ProducerConfig.acks set to One or All")
            error.raiseError[F, Offset]
          } {
            _.pure[F]
          }
        } yield {
          PartitionOffset(partition, offset)
        }
      }
    }
  }


  implicit class AppendActionOps[F[_]](val self: AppendAction[F]) extends AnyVal {

    def mapK[G[_]](f: F ~> G): AppendAction[G] = new AppendAction[G] {
      def apply(action: Action) = f(self(action))
    }
  }
}