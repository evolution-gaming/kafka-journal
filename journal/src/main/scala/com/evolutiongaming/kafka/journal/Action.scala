package com.evolutiongaming.kafka.journal

import java.time.Instant

import cats.data.{NonEmptyList => Nel}
import com.evolutiongaming.kafka.journal.EventsSerializer.EventsToPayload

sealed abstract class Action extends Product {

  def key: Key

  def timestamp: Instant

  def header: ActionHeader

  def origin: Option[Origin] = header.origin
}

object Action {

  sealed abstract class User extends Action

  sealed abstract class System extends Action

  
  final case class Append(
    key: Key,
    timestamp: Instant,
    header: ActionHeader.Append,
    payload: Payload.Binary,
    headers: Headers
  ) extends User {

    def payloadType: PayloadType.BinaryOrJson = header.payloadType

    def range: SeqRange = header.range
  }

  object Append {
    def apply(
      key: Key,
      timestamp: Instant,
      origin: Option[Origin],
      events: Nel[Event],
      metadata: Metadata,
      headers: Headers
    ): Append = {
      val (payload, payloadType) = EventsToPayload(events)
      val range = SeqRange(from = events.head.seqNr, to = events.last.seqNr)
      val header = ActionHeader.Append(range, origin, payloadType, metadata)
      Action.Append(key, timestamp, header, payload, headers)
    }
  }


  final case class Delete(
    key: Key,
    timestamp: Instant,
    header: ActionHeader.Delete
  ) extends User {

    def to: SeqNr = header.to
  }

  object Delete {
    def apply(key: Key, timestamp: Instant, to: SeqNr, origin: Option[Origin]): Delete = {
      val header = ActionHeader.Delete(to, origin)
      Delete(key, timestamp, header)
    }
  }


  final case class Mark(
    key: Key,
    timestamp: Instant,
    header: ActionHeader.Mark
  ) extends System {

    def id: String = header.id
  }

  object Mark {
    def apply(key: Key, timestamp: Instant, id: String, origin: Option[Origin]): Mark = {
      Mark(key, timestamp, ActionHeader.Mark(id, origin))
    }
  }
}