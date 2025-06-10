package com.evolutiongaming.kafka.journal

import cats.{Eq, Order, Show}
import com.evolutiongaming.skafka.consumer.ConsumerRecord
import com.evolutiongaming.skafka.{Offset, Partition}

final case class PartitionOffset(
  partition: Partition = Partition.min,
  offset: Offset = Offset.min,
) {
  override def toString = s"$partition:$offset"
}

object PartitionOffset {

  val empty: PartitionOffset = PartitionOffset()

  implicit val eqPartitionOffset: Eq[PartitionOffset] = Eq.fromUniversalEquals

  implicit val showPartitionOffset: Show[PartitionOffset] = Show.fromToString[PartitionOffset]

  implicit val orderPartitionOffset: Order[PartitionOffset] =
    Order.whenEqual(Order.by { (a: PartitionOffset) => a.partition }, Order.by { (a: PartitionOffset) => a.offset })

  def apply(record: ConsumerRecord[?, ?]): PartitionOffset = {
    PartitionOffset(partition = record.topicPartition.partition, offset = record.offset)
  }
}
