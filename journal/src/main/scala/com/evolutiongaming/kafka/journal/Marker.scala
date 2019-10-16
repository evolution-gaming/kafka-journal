package com.evolutiongaming.kafka.journal

import com.evolutiongaming.skafka.{Offset, Partition}

final case class Marker(id: String, partitionOffset: PartitionOffset) {

  def offset: Offset = partitionOffset.offset

  def partition: Partition = partitionOffset.partition
}