package com.evolutiongaming.kafka.journal

import java.time.Instant

import com.evolutiongaming.skafka.{Offset, Partition}

/**
 * @param origin identifier of event origin, for instance node IP address
 */
final case class EventRecord(
  event: Event,
  timestamp: Instant,
  partitionOffset: PartitionOffset,
  origin: Option[Origin] = None,
  metadata: RecordMetadata,
  headers: Headers
) {

  def seqNr: SeqNr = event.seqNr

  def offset: Offset = partitionOffset.offset

  def partition: Partition = partitionOffset.partition

  def pointer: JournalPointer = JournalPointer(partitionOffset, event.seqNr)
}

object EventRecord {

  def apply(
    record: ActionRecord[Action.Append],
    event: Event,
    metadata: PayloadMetadata
  ): EventRecord = {
    apply(record.action, event, record.partitionOffset, metadata)
  }

  def apply(
    action: Action.Append,
    event: Event,
    partitionOffset: PartitionOffset,
    metadata: PayloadMetadata,
  ): EventRecord = {
    val expireAfter = metadata.expireAfter orElse action.header.expireAfter
    EventRecord(
      event = event,
      timestamp = action.timestamp,
      partitionOffset = partitionOffset,
      origin = action.origin,
      metadata = RecordMetadata(
        header = action.header.metadata,
        payload = metadata.copy(expireAfter = expireAfter)),
      headers = action.headers)
  }
}