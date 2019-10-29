package com.evolutiongaming.kafka.journal

import java.time.Instant

import cats.Functor
import cats.data.{NonEmptyList => Nel}
import cats.implicits._
import com.evolutiongaming.kafka.journal.conversions.EventsToPayload
import scodec.bits.ByteVector

import scala.concurrent.duration.FiniteDuration

sealed abstract class Action extends Product {

  def key: Key

  def timestamp: Instant

  def header: ActionHeader

  def origin: Option[Origin] = header.origin
}

object Action {

  def append(
    key: Key,
    timestamp: Instant,
    header: ActionHeader.Append,
    payload: ByteVector,
    headers: Headers
  ): Action = {
    Append(key, timestamp, header, payload, headers)
  }

  def delete(
    key: Key,
    timestamp: Instant,
    header: ActionHeader.Delete
  ): Action = {
    Delete(key, timestamp, header)
  }

  def mark(
    key: Key,
    timestamp: Instant,
    header: ActionHeader.Mark
  ): Action = {
    Mark(key, timestamp, header)
  }


  sealed abstract class User extends Action

  sealed abstract class System extends Action

  
  final case class Append(
    key: Key,
    timestamp: Instant,
    header: ActionHeader.Append,
    payload: ByteVector,
    headers: Headers
  ) extends User {

    def payloadType: PayloadType.BinaryOrJson = header.payloadType

    def range: SeqRange = header.range
  }

  object Append {

    def of[F[_] : Functor](
      key: Key,
      timestamp: Instant,
      origin: Option[Origin],
      events: Events,
      expireAfter: Option[FiniteDuration],
      metadata: Metadata,
      headers: Headers)(implicit
      eventsToPayload: EventsToPayload[F]
    ): F[Append] = {
      for {
        payloadAndType <- eventsToPayload(events)
      } yield {
        val range = SeqRange(from = events.events.head.seqNr, to = events.events.last.seqNr)
        val header = ActionHeader.Append(
          range = range,
          origin = origin,
          payloadType = payloadAndType.payloadType,
          metadata = metadata,
          expireAfter = expireAfter)
        Action.Append(
          key = key,
          timestamp = timestamp,
          header = header,
          payload = payloadAndType.payload,
          headers = headers)
      }
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
      val header = ActionHeader.Mark(id, origin)
      Mark(key, timestamp, header)
    }
  }
}