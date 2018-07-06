package com.evolutiongaming.kafka.journal.eventual

import java.time.Instant

import com.evolutiongaming.kafka.journal.Alias._
import com.evolutiongaming.skafka.{Offset, Partition}
import scala.collection.immutable.Seq

// TODO rename
// TODO remove id
case class EventualRecord(
  id: Id,
  seqNr: SeqNr,
  timestamp: Instant,
  payload: Bytes,
  tags: Tags,
  partitionOffset: PartitionOffset)


// TODO make own collection
case class EventualRecords(records: Seq[EventualRecord], deletedTo: Option[SeqNr])


sealed trait UpdateTmp

// TODO rename
object UpdateTmp {
  // TODO handle case when there were no deletions
    case class DeleteToKnown(value: SeqNr, records: Seq[EventualRecord]) extends UpdateTmp

  // TODO consider creating case class for unbounded deletedTo
    case class DeleteToUnknown(value: SeqNr) extends UpdateTmp
}


case class PartitionOffset(partition: Partition, offset: Offset)

case class Pointer(
  seqNr: SeqNr,
  partitionOffset: PartitionOffset)