package com.evolutiongaming.kafka.journal

import java.time.Instant

import cats.Functor
import com.evolutiongaming.skafka.{Offset, Partition}

/**
 * @param origin identifier of event origin, for instance node IP address
 */
final case class EventRecord[A](
  event: Event[A],
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

  def apply[A](
    record: ActionRecord[Action.Append],
    event: Event[A],
    metadata: PayloadMetadata
  ): EventRecord[A] = {
    apply(record.action, event, record.partitionOffset, metadata)
  }

  def apply[A](
    action: Action.Append,
    event: Event[A],
    partitionOffset: PartitionOffset,
    metadata: PayloadMetadata,
  ): EventRecord[A] = {
    EventRecord(
      event = event,
      timestamp = action.timestamp,
      partitionOffset = partitionOffset,
      origin = action.origin,
      metadata = RecordMetadata(
        header = action.header.metadata,
        payload = metadata),
      headers = action.headers)
  }

  implicit val functorEventRecord: Functor[EventRecord] = new Functor[EventRecord] {
    override def map[A, B](fa: EventRecord[A])(f: A => B): EventRecord[B] =
      fa.copy(event = Functor[Event].map(fa.event)(f))
  }
}