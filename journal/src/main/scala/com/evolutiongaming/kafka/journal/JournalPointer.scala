package com.evolutiongaming.kafka.journal

import com.evolutiongaming.skafka.{Offset, Partition}

final case class JournalPointer(partitionOffset: PartitionOffset, seqNr: SeqNr) {

  def offset: Offset = partitionOffset.offset

  def partition: Partition = partitionOffset.partition
}
