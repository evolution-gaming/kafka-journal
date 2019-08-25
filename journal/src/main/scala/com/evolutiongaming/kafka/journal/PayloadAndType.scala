package com.evolutiongaming.kafka.journal


import cats.Monad
import cats.data.{NonEmptyList => Nel}
import cats.implicits._
import com.evolutiongaming.catshelper.FromTry
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

    def apply[F[_] : Monad : FromTry /*TODO*/](events: Nel[Event]): F[PayloadAndType] = {

      def eventJson(head: Event) = {

        def ofOpt(payloadType: Option[PayloadType.TextOrJson], payload: Option[JsValue]) = {
          EventJson(head.seqNr, head.tags, payloadType, payload)
        }

        def of[A: Writes](payloadType: PayloadType.TextOrJson, a: A) = {
          for {
            jsValue <- FromTry[F].unsafe { Json.toJson(a) }
          } yield {
            ofOpt(payloadType.some, jsValue.some)
          }
        }

        head.payload.fold {
          ofOpt(none, none).pure[F].some
        } {
          case _: Payload.Binary => none[F[EventJson]]
          case a: Payload.Json   => of(PayloadType.Json, a.value).some
          case a: Payload.Text   => of(PayloadType.Text, a.value).some
        }
      }

      @tailrec
      def eventJsons(events: List[Event], json: List[F[EventJson]]): F[List[EventJson]] = {
        events match {
          case Nil          => json.foldLeftM(List.empty[EventJson]) { (as, a) => a.map { _ :: as } }
          case head :: tail => eventJson(head) match {
            case None    => List.empty[EventJson].pure[F]
            case Some(x) => eventJsons(tail, x :: json)
          }
        }
      }

      def payloadAndType(eventJsons: List[EventJson]) = {
        eventJsons match {
          case head :: tail =>
            val payload = PayloadJson(Nel(head, tail))
            for {
              bytes <- FromTry[F].unsafe { payload.toBytes }
            } yield {
              val byteVector = ByteVector.view(bytes)
              PayloadAndType(byteVector, PayloadType.Json)
            }
          case Nil =>
            for {
              bytes <- FromTry[F].unsafe { events.toBytes }
            } yield {
              val byteVector = ByteVector.view(bytes)
              PayloadAndType(byteVector, PayloadType.Binary)
            }
        }
      }

      for {
        eventJsons     <- eventJsons(events.toList, List.empty)
        payloadAndType <- payloadAndType(eventJsons)
      } yield payloadAndType
    }
  }


  object EventsFromPayload {

    def apply[F[_] : Monad : FromTry /*TODO*/ ](payloadAndType: PayloadAndType): F[Nel[Event]] = {
      val payload = payloadAndType.payload.toArray // TODO avoid calling this, work with ByteVector
      payloadAndType.payloadType match {
        case PayloadType.Binary =>
          FromTry[F].unsafe { payload.fromBytes[Nel[Event]] } // TODO avoid using toBytes/fromBytes

        case PayloadType.Json =>
          for {
            payloadJson <- FromTry[F].unsafe { payload.fromBytes[PayloadJson] }
          } yield {
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