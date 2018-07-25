package com.evolutiongaming.kafka.journal.eventual

import com.evolutiongaming.kafka.journal.Alias._
import com.evolutiongaming.kafka.journal.ReplicatedEvent
import com.evolutiongaming.skafka.{Offset, Partition}


sealed trait UpdateTmp

// TODO rename
object UpdateTmp {
  final case class DeleteToKnown(value: Option[SeqNr], replicated: List[ReplicatedEvent]) extends UpdateTmp

  // TODO consider creating case class for unbounded deletedTo
  final case class DeleteUnbound(value: SeqNr) extends UpdateTmp
}


final case class PartitionOffset(partition: Partition, offset: Offset) {
  override def toString = s"$partition:$offset"
}

object PartitionOffset {
  val Empty: PartitionOffset = PartitionOffset(0, 0l)
}


final case class Pointer(
  seqNr: SeqNr,
  partitionOffset: PartitionOffset)