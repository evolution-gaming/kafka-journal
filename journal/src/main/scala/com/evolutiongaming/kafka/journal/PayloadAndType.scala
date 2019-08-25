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

  def eventsToBytes[F[_] : FromTry/*TODO*/]: Conversion[F, Nel[Event], Bytes] = {
    a: Nel[Event] => FromTry[F].unsafe { a.toBytes } // TODO
  }

  def payloadJsonToBytes[F[_] : FromTry/*TODO*/]: Conversion[F, PayloadJson, Bytes] = {
    a: PayloadJson => FromTry[F].unsafe { a.toBytes } // TODO avoid using toBytes/fromBytes
  }

  def eventsToPayloadAndType[F[_] : Monad : FromTry](implicit
    eventsToBytes: Conversion[F, Nel[Event], Bytes],
    payloadJsonToBytes: Conversion[F, PayloadJson, Bytes]
  ): Conversion[F, Nel[Event], PayloadAndType] = {
    events: Nel[Event] => {

      def eventJson(head: Event) = {

        def ofOpt(payloadType: Option[PayloadType.TextOrJson], payload: Option[JsValue]) = {
          EventJson(head.seqNr, head.tags, payloadType, payload)
        }

        def of[A: Writes](payloadType: PayloadType.TextOrJson, a: A) = {
          for {
            jsValue <- FromTry[F].unsafe { Json.toJson(a) } // TODO
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
            val events = Nel(head, tail)
            val payload = PayloadJson(events)
            for {
              bytes <- payloadJsonToBytes(payload)
            } yield {
              val byteVector = ByteVector.view(bytes)
              PayloadAndType(byteVector, PayloadType.Json)
            }
          case Nil          =>
            for {
              bytes <- eventsToBytes(events)
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


  def bytesToEvents[F[_] : FromTry/*TODO*/]: Conversion[F, Bytes, Nel[Event]] = {
    a: Bytes => FromTry[F].unsafe { a.fromBytes[Nel[Event]] } // TODO
  }

  def bytesToPayloadJson[F[_] : FromTry/*TODO*/]: Conversion[F, Bytes, PayloadJson] = {
    a: Bytes => FromTry[F].unsafe { a.fromBytes[PayloadJson] } // TODO avoid using toBytes/fromBytes
  }

  def payloadAndTypeToEvents[F[_] : Monad : FromTry /*TODO*/ : FromAttempt : FromJsResult](implicit
    bytesToEvents: Conversion[F, Bytes, Nel[Event]],
    bytesToPayloadJson: Conversion[F, Bytes, PayloadJson]
  ): Conversion[F, PayloadAndType, Nel[Event]] = {

    payloadAndType: PayloadAndType => {
      val payload = payloadAndType.payload.toArray // TODO avoid calling this, work with ByteVector
      payloadAndType.payloadType match {
        case PayloadType.Binary => bytesToEvents(payload)
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
            payloadJson <- bytesToPayloadJson(payload)
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


    implicit val ToBytesPayloadJson: ToBytes[PayloadJson] = ToBytes.fromWrites

    implicit val FromBytesPayloadJson: FromBytes[PayloadJson] = FromBytes.fromReads
  }
}