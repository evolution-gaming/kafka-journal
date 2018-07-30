package com.evolutiongaming.kafka.journal

import com.evolutiongaming.skafka.consumer.ConsumerRecord
import com.evolutiongaming.skafka.{Offset, Partition}

final case class PartitionOffset(partition: Partition, offset: Offset) {
  override def toString = s"$partition:$offset"
}

object PartitionOffset {
  val Empty: PartitionOffset = PartitionOffset(0, 0l)

  def apply(record: ConsumerRecord[_, _]): PartitionOffset = {
    PartitionOffset(
      partition = record.topicPartition.partition,
      offset = record.offset)
  }
}