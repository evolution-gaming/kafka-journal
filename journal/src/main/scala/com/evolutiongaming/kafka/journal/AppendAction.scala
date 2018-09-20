package com.evolutiongaming.kafka.journal

import com.evolutiongaming.concurrent.async.Async
import com.evolutiongaming.concurrent.async.AsyncConverters._
import com.evolutiongaming.kafka.journal.KafkaConverters._
import com.evolutiongaming.skafka.producer.Producer

import scala.concurrent.ExecutionContext

trait AppendAction[F[_]] {
  def apply(action: Action): F[PartitionOffset]
}

object AppendAction {

  def apply(producer: Producer)(implicit ec: ExecutionContext): AppendAction[Async] = new AppendAction[Async] {

    def apply(action: Action) = {
      val producerRecord = action.toProducerRecord
      for {
        metadata <- producer.send(producerRecord).async
      } yield {
        val partition = metadata.topicPartition.partition
        val offset = metadata.offset getOrElse {
          throw JournalException(action.key, "metadata.offset is missing, make sure ProducerConfig.acks set to One or All")
        }
        PartitionOffset(partition, offset)
      }
    }
  }
}