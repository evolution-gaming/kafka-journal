package com.evolutiongaming.kafka.journal


import cats.data.{NonEmptyList => Nel}
import cats.implicits._
import com.evolutiongaming.kafka.journal.FromBytes.Implicits._
import com.evolutiongaming.kafka.journal.PlayJsonHelper._
import com.evolutiongaming.kafka.journal.ToBytes.Implicits._
import com.evolutiongaming.kafka.journal.util.ScodecHelper._
import play.api.libs.json._
import scodec.Codec
import scodec.bits.ByteVector

import scala.annotation.tailrec

final case class PayloadAndType(
  payload: ByteVector,
  payloadType: PayloadType.BinaryOrJson)

object PayloadAndType {

  object EventsToPayload {

    def apply(events: Nel[Event]): PayloadAndType = {

      @tailrec
      def loop(events: List[Event], json: List[EventJson]): List[EventJson] = {
        events match {
          case Nil          => json.reverse
          case head :: tail =>

            def ofOpt(payloadType: Option[PayloadType.TextOrJson], payload: Option[JsValue]) = {
              EventJson(head.seqNr, head.tags, payloadType, payload)
            }

            def of[A : Writes](payloadType: PayloadType.TextOrJson, a: A) = {
              val jsValue = Json.toJson(a)
              ofOpt(payloadType.some, jsValue.some)
            }

            val result = head.payload.fold[Option[EventJson]]{
              ofOpt(none, none).some
            } {
              case _: Payload.Binary => none[EventJson]
              case a: Payload.Json   => of(PayloadType.Json, a.value).some
              case a: Payload.Text   => of(PayloadType.Text, a.value).some
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
          PayloadAndType(byteVector, PayloadType.Binary)

        case head :: tail =>
          val payload = PayloadJson(Nel(head, tail))
          val bytes = payload.toBytes
          val byteVector = ByteVector.view(bytes)
          PayloadAndType(byteVector, PayloadType.Json)
      }
    }
  }


  object EventsFromPayload {

    def apply(payloadAndType: PayloadAndType): Nel[Event] = {
      val payload = payloadAndType.payload.toArray // TODO avoid calling this, work with ByteVector
      payloadAndType.payloadType match {
        case PayloadType.Binary =>
          payload.fromBytes[Nel[Event]] // TODO avoid using toBytes/fromBytes
          
        case PayloadType.Json   =>
          val payloadJson = payload.fromBytes[PayloadJson]
          for {
            event <- payloadJson.events
          } yield {
            val payloadType = event.payloadType getOrElse PayloadType.Json
            val payload = event.payload.map { payload =>
              payloadType match {
                case PayloadType.Json => Payload.json(payload)
                case PayloadType.Text => Payload.text(payload.as[String]) // TODO not use `as`
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

    implicit val FormatEventJson: OFormat[EventJson] = Json.format


    implicit val WritesNelEventJson: Writes[Nel[EventJson]] = nelWrites

    implicit val ReadsNelEventJson: Reads[Nel[EventJson]] = nelReads
  }


  final case class PayloadJson(events: Nel[EventJson])

  object PayloadJson {

    implicit val FormatPayloadJson: OFormat[PayloadJson] = Json.format


    implicit val CodecPayloadJson: Codec[PayloadJson] = formatCodec


    implicit val ToBytesPayloadJson: ToBytes[PayloadJson] = ToBytes.fromWrites

    implicit val FromBytesPayloadJson: FromBytes[PayloadJson] = FromBytes.fromReads
  }
}