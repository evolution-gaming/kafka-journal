package com.evolutiongaming.kafka.journal


import cats.{Applicative, Monad}
import cats.data.{NonEmptyList => Nel}
import cats.implicits._
import com.evolutiongaming.kafka.journal.PlayJsonHelper._
import com.evolutiongaming.kafka.journal.util.ScodecHelper._
import play.api.libs.json._
import scodec.Codec
import scodec.bits.ByteVector

import scala.annotation.tailrec

final case class PayloadAndType(
  payload: ByteVector,
  payloadType: PayloadType.BinaryOrJson)

object PayloadAndType {

  def eventsToPayload[F[_] : Monad](implicit
    eventsToBytes: ToBytes[F, Nel[Event]],
    payloadJsonToBytes: ToBytes[F, PayloadJson]
  ): Conversion[F, Nel[Event], PayloadAndType] = {
    events: Nel[Event] => {

      def eventJson(head: Event) = {

        def ofOpt(payloadType: Option[PayloadType.TextOrJson], payload: Option[JsValue]) = {
          EventJson(head.seqNr, head.tags, payloadType, payload)
        }

        def of[A](payloadType: PayloadType.TextOrJson, a: A)(implicit writes: Writes[A]) = {
          val jsValue = writes.writes(a)
          ofOpt(payloadType.some, jsValue.some)
        }

        head.payload.fold {
          ofOpt(none, none).some
        } {
          case _: Payload.Binary => none[EventJson]
          case a: Payload.Json   => of(PayloadType.Json, a.value).some
          case a: Payload.Text   => of(PayloadType.Text, a.value).some
        }
      }

      @tailrec
      def eventJsons(events: List[Event], result: List[EventJson]): List[EventJson] = {
        events match {
          case Nil          => result.reverse
          case head :: tail => eventJson(head) match {
            case None    => List.empty[EventJson]
            case Some(x) => eventJsons(tail, x :: result)
          }
        }
      }

      def payloadAndType(eventJsons: List[EventJson]) = {
        eventJsons match {
          case head :: tail =>
            val events = Nel(head, tail)
            val payload = PayloadJson(events)
            for {
              bytes <- payloadJsonToBytes(payload)
            } yield {
              PayloadAndType(bytes, PayloadType.Json)
            }
          case Nil          =>
            for {
              bytes <- eventsToBytes(events)
            } yield {
              PayloadAndType(bytes, PayloadType.Binary)
            }
        }
      }

      payloadAndType(eventJsons(events.toList, List.empty))
    }
  }


  def payloadToEvents[F[_] : Monad : FromAttempt : FromJsResult](implicit
    eventsFromBytes: FromBytes[F, Nel[Event]],
    payloadJsonFromBytes: FromBytes[F, PayloadJson]
  ): Conversion[F, PayloadAndType, Nel[Event]] = {

    payloadAndType: PayloadAndType => {
      val payload = payloadAndType.payload
      payloadAndType.payloadType match {
        case PayloadType.Binary => eventsFromBytes(payload)
        case PayloadType.Json   =>

          def events(payloadJson: PayloadJson) = {
            payloadJson.events.traverse { event =>
              val payloadType = event.payloadType getOrElse PayloadType.Json
              val payload = event.payload.traverse { payload =>

                def text = {
                  for {
                    str <- FromJsResult[F].apply { payload.validate[String] }
                  } yield {
                    Payload.text(str)
                  }
                }

                payloadType match {
                  case PayloadType.Json => Payload.json(payload).pure[F]
                  case PayloadType.Text => text
                }
              }
              for {
                payload <- payload
              } yield {
                Event(
                  seqNr = event.seqNr,
                  tags = event.tags,
                  payload = payload)
              }
            }
          }

          for {
            payloadJson <- payloadJsonFromBytes(payload)
            events      <- events(payloadJson)
          } yield events
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


    implicit val CodecPayloadJson: Codec[PayloadJson] = formatCodec // TODO not used


    implicit def toBytesPayloadJson[F[_] : Applicative]: ToBytes[F, PayloadJson] = ToBytes.fromWrites

    implicit def fromBytesPayloadJson[F[_] : FromJsResult]: FromBytes[F, PayloadJson] = FromBytes.fromReads
  }
}