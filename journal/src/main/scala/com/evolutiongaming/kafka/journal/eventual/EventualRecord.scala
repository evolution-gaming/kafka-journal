package com.evolutiongaming.kafka.journal.eventual

import java.time.Instant

import com.evolutiongaming.kafka.journal.Alias._
import com.evolutiongaming.skafka.{Offset, Partition}
import scala.collection.immutable.Seq

// TODO rename
// TODO remove id
// TODO use Event
case class EventualRecord(
  id: Id,
  seqNr: SeqNr,
  timestamp: Instant,
  payload: Bytes,
  tags: Tags,
  partitionOffset: PartitionOffset)


sealed trait UpdateTmp

// TODO rename
object UpdateTmp {
    case class DeleteToKnown(value: Option[SeqNr], records: Seq[EventualRecord]) extends UpdateTmp

  // TODO consider creating case class for unbounded deletedTo
    case class DeleteUnbound(value: SeqNr) extends UpdateTmp
}


case class PartitionOffset(partition: Partition, offset: Offset)

case class Pointer(
  seqNr: SeqNr,
  partitionOffset: PartitionOffset)