package com.evolutiongaming.kafka.journal.eventual

import java.time.Instant

import com.evolutiongaming.kafka.journal.Alias._
import com.evolutiongaming.skafka.{Offset, Partition}

case class EventualRecord(
  id: Id,
  seqNr: SeqNr,
  timestamp: Instant,
  payload: Bytes,
  tags: Tags,
  partitionOffset: PartitionOffset)

case class PartitionOffset(partition: Partition, offset: Offset)

case class Pointer(
  seqNr: SeqNr,
  partitionOffset: PartitionOffset)