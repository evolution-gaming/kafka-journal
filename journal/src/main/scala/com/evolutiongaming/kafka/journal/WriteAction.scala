package com.evolutiongaming.kafka.journal

import com.evolutiongaming.kafka.journal.Alias.Id
import com.evolutiongaming.kafka.journal.KafkaConverters._
import com.evolutiongaming.skafka.producer.Producer
import com.evolutiongaming.skafka.{Offset, Partition, Topic}

import scala.concurrent.{ExecutionContext, Future}

trait WriteAction {
  def apply(action: Action): Future[(Partition, Option[Offset])]
}

object WriteAction {

  def apply(
    id: Id,
    topic: Topic,
    producer: Producer)(implicit
    ec: ExecutionContext): WriteAction = {

    new WriteAction {
      def apply(action: Action) = {
        val kafkaRecord = KafkaRecord(id, topic, action)
        val producerRecord = kafkaRecord.toProducerRecord
        for {
          metadata <- producer(producerRecord)
        } yield {
          val partition = metadata.topicPartition.partition
          (partition, metadata.offset)
        }
      }
    }
  }
}
