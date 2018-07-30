package com.evolutiongaming.kafka.journal

import com.evolutiongaming.concurrent.async.Async
import com.evolutiongaming.concurrent.async.AsyncConverters._
import com.evolutiongaming.kafka.journal.KafkaConverters._
import com.evolutiongaming.skafka.producer.Producer

import scala.concurrent.ExecutionContext

trait WriteAction {
  def apply(action: Action): Async[PartitionOffset]
}

object WriteAction {

  def apply(
    key: Key,
    producer: Producer)(implicit
    ec: ExecutionContext): WriteAction = {

    new WriteAction {
      def apply(action: Action) = {
        val kafkaRecord = KafkaRecord(key, action)
        val producerRecord = kafkaRecord.toProducerRecord
        for {
          metadata <- producer(producerRecord).async
        } yield {
          val partition = metadata.topicPartition.partition
          val offset = metadata.offset getOrElse {
            throw JournalException(key, "metadata.offset is missing, make sure ProducerConfig.acks set to One or All")
          }
          PartitionOffset(partition, offset)
        }
      }
    }
  }
}
