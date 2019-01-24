package com.evolutiongaming.kafka.journal

import java.lang.{Byte => ByteJ, Integer => IntJ, Long => LongJ}
import java.nio.ByteBuffer

import com.evolutiongaming.kafka.journal.FromBytes.Implicits._
import com.evolutiongaming.kafka.journal.PlayJsonHelper._
import com.evolutiongaming.kafka.journal.Tags._
import com.evolutiongaming.kafka.journal.ToBytes.Implicits._
import com.evolutiongaming.kafka.journal.util.ByteBufferHelper._
import com.evolutiongaming.nel.Nel
import play.api.libs.json._

import scala.annotation.tailrec

object EventsSerializer {

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


  implicit class ByteBufferNelOps(val self: ByteBuffer) extends AnyVal {

    def readNel[T](f: => T): Nel[T] = {
      val length = self.getInt()
      val list = List.fill(length) {
        val length = self.getInt
        val position = self.position()
        val value = f
        self.position(position + length)
        value
      }
      Nel.unsafe(list)
    }

    def writeNel(bytes: Nel[Bytes]): Unit = {
      self.putInt(bytes.length)
      bytes.foreach { bytes => self.writeBytes(bytes) }
    }
  }


  object EventsToPayload {

    def apply(events: Nel[Event]): (Payload.Binary, PayloadType.BinaryOrJson) = {

      @tailrec
      def loop(events: List[Event], json: List[EventJson]): List[EventJson] = {
        events match {
          case Nil          => json.reverse
          case head :: tail =>
            val result = head.payload.fold[Option[EventJson]](Some(EventJson(head))) {
              case _: Payload.Binary => None
              case a: Payload.Text   => Some(EventJson(head, a))
              case a: Payload.Json   => Some(EventJson(head, a))
            }
            result match {
              case None    => Nil
              case Some(x) => loop(tail, x :: json)
            }
        }
      }

      loop(events.toList, Nil) match {
        case Nil =>
          val bytes = events.toBytes
          (Payload.Binary(bytes), PayloadType.Binary)

        case head :: tail =>
          val payload = PayloadJson(Nel(head, tail))
          val json = Json.toJson(payload)
          val bytes = json.toBytes
          (Payload.Binary(bytes), PayloadType.Json)
      }
    }
  }


  object EventsFromPayload {

    def apply(payload: Payload.Binary, payloadType: PayloadType.BinaryOrJson): Nel[Event] = {
      payloadType match {
        case PayloadType.Binary => payload.value.fromBytes[Nel[Event]]
        case PayloadType.Json   =>
          val json = payload.value.fromBytes[JsValue]
          val payloadJson = json.as[PayloadJson]
          for {
            event <- payloadJson.events
          } yield {
            val payloadType = event.payloadType getOrElse PayloadType.Json
            val payload = event.payload.map { payload =>
              payloadType match {
                case PayloadType.Json => Payload.Json(payload)
                case PayloadType.Text => Payload.Text(payload.as[String])
              }
            }
            Event(
              seqNr = event.seqNr,
              tags = event.tags,
              payload = payload)
          }
      }
    }
  }


  final case class EventJson(
    seqNr: SeqNr,
    tags: Tags,
    payloadType: Option[PayloadType.TextOrJson] = None,
    payload: Option[JsValue] = None)

  object EventJson {

    implicit val FormatImpl: OFormat[EventJson] = Json.format[EventJson]


    implicit val NelWritesImpl: Writes[Nel[EventJson]] = nelWrites[EventJson]

    implicit val NelReadsImpl: Reads[Nel[EventJson]] = nelReads[EventJson]


    def apply(event: Event): EventJson = {
      EventJson(
        seqNr = event.seqNr,
        tags = event.tags)
    }

    def apply(event: Event, payload: Payload.Json): EventJson = {
      EventJson(
        seqNr = event.seqNr,
        tags = event.tags,
        payload = Some(payload.value))
    }

    def apply(event: Event, payload: Payload.Text): EventJson = {
      EventJson(
        seqNr = event.seqNr,
        tags = event.tags,
        payloadType = Some(PayloadType.Text),
        payload = Some(JsString(payload.value)))
    }
  }


  final case class PayloadJson(events: Nel[EventJson])

  object PayloadJson {
    implicit val FormatImpl: OFormat[PayloadJson] = Json.format[PayloadJson]
  }
}