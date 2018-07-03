package com.evolutiongaming.kafka.journal.ally

import java.time.Instant

import com.evolutiongaming.kafka.journal.Alias._
import com.evolutiongaming.skafka.{Offset, Partition}

case class AllyRecord(
  id: Id,
  seqNr: SeqNr,
  timestamp: Instant,
  payload: Bytes,
  tags: Tags,
  partitionOffset: PartitionOffset)

case class PartitionOffset(partition: Partition, offset: Offset)

case class AllyRecord2(
  seqNr: SeqNr,
  partitionOffset: PartitionOffset)