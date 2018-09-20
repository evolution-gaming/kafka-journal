package com.evolutiongaming.kafka.journal

import java.time.Instant

import com.evolutiongaming.kafka.journal.EventsSerializer.EventsToPayload
import com.evolutiongaming.nel.Nel
import com.evolutiongaming.skafka.{Offset, Partition}

trait Action {
  def key: Key
  def timestamp: Instant
  def origin: Option[Origin]
}

object Action {

  sealed trait User extends Action

  sealed trait System extends Action

  final case class Append(
    key: Key,
    timestamp: Instant,
    origin: Option[Origin],
    range: SeqRange,
    payloadType: PayloadType.BinaryOrJson,
    payload: Payload.Binary) extends User

  object Append {
    def apply(key: Key, timestamp: Instant, origin: Option[Origin], events: Nel[Event]): Append = {
      val (payload, payloadType) = EventsToPayload(events)
      val range = SeqRange(from = events.head.seqNr, to = events.last.seqNr)
      Action.Append(key, timestamp, origin, range, payloadType, payload)
    }
  }

  final case class Delete(
    key: Key,
    timestamp: Instant,
    origin: Option[Origin],
    to: SeqNr) extends User

  final case class Mark(
    key: Key,
    timestamp: Instant,
    origin: Option[Origin],
    id: String) extends System
}


final case class ActionRecord(action: Action, partitionOffset: PartitionOffset) {

  def offset: Offset = partitionOffset.offset

  def partition: Partition = partitionOffset.partition
}