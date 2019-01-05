package com.evolutiongaming.kafka.journal

import cats.effect.Sync
import cats.implicits._
import com.evolutiongaming.concurrent.async.Async
import com.evolutiongaming.kafka.journal.KafkaConverters._
import com.evolutiongaming.kafka.journal.util.ToFuture
import com.evolutiongaming.skafka.Offset

import scala.concurrent.ExecutionContext

trait AppendAction[F[_]] {
  def apply(action: Action): F[PartitionOffset]
}

object AppendAction {

  def apply[F[_] : Sync](producer: KafkaProducer[F]): AppendAction[F] = {

    new AppendAction[F] {

      def apply(action: Action) = {
        val producerRecord = action.toProducerRecord
        for {
          metadata  <- producer.send(producerRecord)
          partition  = metadata.topicPartition.partition
          offset    <- metadata.offset.fold {
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

  def async[F[_] : Sync : ToFuture](producer: KafkaProducer[F])(implicit ec: ExecutionContext): AppendAction[Async] = {

    val appendAction = apply[F](producer)

    new AppendAction[Async] {
      def apply(action: Action) = {
        Async {
          ToFuture[F].apply {
            appendAction(action)
          }
        }
      }
    }
  }
}