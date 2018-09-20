package com.evolutiongaming.kafka.journal

import java.time.Instant

import com.evolutiongaming.kafka.journal.Alias.Tags
import com.evolutiongaming.skafka.{Offset, Partition}

// TODO add timestamp ?
final case class Event(
  seqNr: SeqNr,
  tags: Tags = Tags.Empty,
  payload: Bytes = Bytes.Empty)

// TODO rename, statements called records, here - events
// TODO move to eventual ?
// TODO add Key
/**
  * @param origin identifier of event origin, for instance node IP address
  */
final case class ReplicatedEvent(
  event: Event,
  timestamp: Instant,
  partitionOffset: PartitionOffset,
  origin: Option[Origin] = None) {

  def seqNr: SeqNr = event.seqNr

  def offset: Offset = partitionOffset.offset

  def partition: Partition = partitionOffset.partition
}