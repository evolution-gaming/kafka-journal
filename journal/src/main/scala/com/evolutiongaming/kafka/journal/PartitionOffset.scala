package com.evolutiongaming.kafka.journal

import com.evolutiongaming.skafka.{Offset, Partition}

final case class PartitionOffset(partition: Partition, offset: Offset) {
  override def toString = s"$partition:$offset"
}

object PartitionOffset {
  val Empty: PartitionOffset = PartitionOffset(0, 0l)
}