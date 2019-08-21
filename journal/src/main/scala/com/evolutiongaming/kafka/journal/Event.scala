package com.evolutiongaming.kafka.journal

import java.lang.{Byte => ByteJ, Integer => IntJ, Long => LongJ}
import java.nio.ByteBuffer

import cats.data.{NonEmptyList => Nel}
import com.evolutiongaming.kafka.journal.FromBytes.Implicits._
import com.evolutiongaming.kafka.journal.Tags._
import com.evolutiongaming.kafka.journal.ToBytes.Implicits._
import com.evolutiongaming.kafka.journal.util.ByteBufferHelper._

final case class Event(
  seqNr: SeqNr,
  tags: Tags = Tags.Empty,
  payload: Option[Payload] = None)

object Event {

  implicit val EventToBytes: ToBytes[Event] = new ToBytes[Event] {

    def apply(event: Event): Bytes = {
      val (payloadType, bytes) = event.payload match {
        case None          => (0: Byte, Bytes.Empty)
        case Some(payload) => payload match {
          case payload: Payload.Binary => (1: Byte, payload.toBytes)
          case payload: Payload.Json   => (2: Byte, payload.toBytes)
          case payload: Payload.Text   => (3: Byte, payload.toBytes)
        }
      }
      val tags = event.tags.toBytes
      val buffer = ByteBuffer.allocate(LongJ.BYTES + IntJ.BYTES + tags.length + ByteJ.BYTES + IntJ.BYTES + bytes.length)
      buffer.putLong(event.seqNr.value)
      buffer.writeBytes(tags)
      buffer.put(payloadType)
      buffer.writeBytes(bytes)
      buffer.array()
    }
  }


  implicit val EventsToBytes: ToBytes[Nel[Event]] = new ToBytes[Nel[Event]] {

    def apply(events: Nel[Event]) = {
      val eventBytes = events.map(_.toBytes)

      val length = eventBytes.foldLeft(ByteJ.BYTES + IntJ.BYTES) { case (length, event) =>
        length + IntJ.BYTES + event.length
      }

      val buffer = ByteBuffer.allocate(length)
      buffer.put(0: Byte) // version
      buffer.writeNel(eventBytes)
      buffer.array()
    }
  }

  implicit val EventsFromBytes: FromBytes[Nel[Event]] = new FromBytes[Nel[Event]] {

    def apply(bytes: Bytes) = {
      val buffer = ByteBuffer.wrap(bytes)
      buffer.get() // version
      buffer.readNel {
        val seqNr = SeqNr(buffer.getLong())
        val tags = buffer.readBytes.fromBytes[Tags]
        val payloadType = buffer.get()
        val bytes = buffer.readBytes
        val payload = payloadType match {
          case 0 => None
          case 1 => Some(bytes.fromBytes[Payload.Binary])
          case 2 => Some(bytes.fromBytes[Payload.Json])
          case 3 => Some(bytes.fromBytes[Payload.Text])
          case _ => Some(bytes.fromBytes[Payload.Binary])
        }
        Event(seqNr, tags, payload)
      }
    }
  }
}