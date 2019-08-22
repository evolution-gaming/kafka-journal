package com.evolutiongaming.kafka.journal


import cats.data.{NonEmptyList => Nel}
import com.evolutiongaming.kafka.journal.FromBytes.Implicits._
import com.evolutiongaming.kafka.journal.PlayJsonHelper._
import com.evolutiongaming.kafka.journal.ToBytes.Implicits._
import play.api.libs.json._
import scodec.bits.ByteVector

import scala.annotation.tailrec

object EventsSerializer {

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
          val byteVector = ByteVector.view(bytes)
          (Payload.Binary(byteVector), PayloadType.Binary)

        case head :: tail =>
          val payload = PayloadJson(Nel(head, tail))
          val json = Json.toJson(payload)
          val bytes = json.toBytes
          val byteVector = ByteVector.view(bytes)
          (Payload.Binary(byteVector), PayloadType.Json)
      }
    }
  }


  object EventsFromPayload {

    def apply(payload: Payload.Binary, payloadType: PayloadType.BinaryOrJson): Nel[Event] = {
      payloadType match {
        case PayloadType.Binary => payload.value.toArray.fromBytes[Nel[Event]] // TODO
        case PayloadType.Json   =>
          val json = payload.value.toArray.fromBytes[JsValue]
          val payloadJson = json.as[PayloadJson]
          for {
            event <- payloadJson.events
          } yield {
            val payloadType = event.payloadType getOrElse PayloadType.Json
            val payload = event.payload.map { payload =>
              payloadType match {
                case PayloadType.Json => Payload.json(payload)
                case PayloadType.Text => Payload.text(payload.as[String])
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

    implicit val FormatEventJson: OFormat[EventJson] = Json.format[EventJson]


    implicit val WritesNelEventJson: Writes[Nel[EventJson]] = nelWrites[EventJson]

    implicit val ReadsNelEventJson: Reads[Nel[EventJson]] = nelReads[EventJson]


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
    implicit val FormatPayloadJson: OFormat[PayloadJson] = Json.format[PayloadJson]
  }
}