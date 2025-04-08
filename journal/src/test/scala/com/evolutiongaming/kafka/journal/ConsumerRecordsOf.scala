package com.evolutiongaming.kafka.journal

import cats.syntax.all.*
import com.evolutiongaming.skafka.consumer.{ConsumerRecord, ConsumerRecords}

object ConsumerRecordsOf {

  def apply[K, V](records: List[ConsumerRecord[K, V]]): ConsumerRecords[K, V] = {
    val records1 = for {
      (topicPartition, records) <- records.groupBy(_.topicPartition)
      records <- records.toNel
    } yield {
      (topicPartition, records)
    }
    ConsumerRecords(records1)
  }

  def apply[K, V](records: ConsumerRecord[K, V]*): ConsumerRecords[K, V] = {
    apply(records.toList)
  }
}
