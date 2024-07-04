package com.evolutiongaming.kafka.journal

import java.time.Instant

import cats.Functor
import cats.syntax.all._
import com.evolutiongaming.kafka.journal.conversions.KafkaWrite
import scodec.bits.ByteVector


sealed abstract class Action extends Product {

  def key: Key

  def timestamp: Instant

  def header: ActionHeader

  def origin: Option[Origin] = header.origin

  def version: Option[Version] = header.version
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

  def purge(
    key: Key,
    timestamp: Instant,
    header: ActionHeader.Purge
  ): Action = {
    Purge(key, timestamp, header)
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

    def of[F[_] : Functor, A](
      key: Key,
      timestamp: Instant,
      origin: Option[Origin],
      version: Option[Version],
      events: Events[A],
      metadata: HeaderMetadata,
      headers: Headers)(implicit
      kafkaWrite: KafkaWrite[F, A]
    ): F[Append] = {
      for {
        payloadAndType <- kafkaWrite(events)
      } yield {
        val range = SeqRange(from = events.events.head.seqNr, to = events.events.last.seqNr)
        val header = ActionHeader.Append(
          range = range,
          origin = origin,
          payloadType = payloadAndType.payloadType,
          metadata = metadata,
          version = version)
        Append(
          key = key,
          timestamp = timestamp,
          header = header,
          payload = payloadAndType.payload,
          headers = headers)
      }
    }

    implicit class AppendOps(val self: Append) extends AnyVal {
      def toPayloadAndType: PayloadAndType = PayloadAndType(self.payload, self.header.payloadType)
    }
  }


  final case class Delete(
    key: Key,
    timestamp: Instant,
    header: ActionHeader.Delete
  ) extends User {

    def to: DeleteTo = header.to
  }

  object Delete {

    def apply(
      key: Key,
      timestamp: Instant,
      to: DeleteTo,
      origin: Option[Origin],
      version: Option[Version]
    ): Delete = {
      val header = ActionHeader.Delete(to, origin, version)
      Delete(key, timestamp, header)
    }
  }


  final case class Purge(
    key: Key,
    timestamp: Instant,
    header: ActionHeader.Purge
  ) extends User

  object Purge {

    def apply(key: Key, timestamp: Instant, origin: Option[Origin], version: Option[Version]): Purge = {
      val header = ActionHeader.Purge(origin, version)
      Purge(key, timestamp, header)
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

    def apply(
      key: Key,
      timestamp: Instant,
      id: String,
      origin: Option[Origin],
      version: Option[Version]
    ): Mark = {
      val header = ActionHeader.Mark(id, origin, version)
      Mark(key, timestamp, header)
    }
  }
}