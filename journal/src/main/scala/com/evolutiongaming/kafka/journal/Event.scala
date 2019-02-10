package com.evolutiongaming.kafka.journal

import java.time.Instant

import com.evolutiongaming.skafka.{Offset, Partition}

final case class Event(
  seqNr: SeqNr,
  tags: Tags = Tags.Empty,
  payload: Option[Payload] = None)


final case class EventRecord(
  event: Event,
  metadata: Metadata)


// TODO rename, statements called records, here - events
// TODO add Key
/**
  * @param origin identifier of event origin, for instance node IP address
  */
final case class ReplicatedEvent(
  event: Event,
  timestamp: Instant,
  partitionOffset: PartitionOffset,
  origin: Option[Origin] = None,
  metadata: Metadata) {

  def seqNr: SeqNr = event.seqNr

  def offset: Offset = partitionOffset.offset

  def partition: Partition = partitionOffset.partition

  def pointer: Pointer = Pointer(partitionOffset, event.seqNr)
}

object ReplicatedEvent {

  def apply(record: ActionRecord[Action.Append], event: Event): ReplicatedEvent = {
    val action = record.action
    ReplicatedEvent(
      event = event,
      timestamp = action.timestamp,
      partitionOffset = record.partitionOffset,
      origin = action.origin,
      metadata = action.header.metadata)
  }
}


final case class Pointer(partitionOffset: PartitionOffset, seqNr: SeqNr) {

  def offset: Offset = partitionOffset.offset

  def partition: Partition = partitionOffset.partition
}