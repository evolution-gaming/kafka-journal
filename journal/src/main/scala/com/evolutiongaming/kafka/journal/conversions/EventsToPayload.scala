package com.evolutiongaming.kafka.journal.conversions

import cats.data.{NonEmptyList => Nel}
import cats.implicits._
import com.evolutiongaming.catshelper.MonadThrowable
import com.evolutiongaming.kafka.journal.PayloadAndType.{EventJson, PayloadJson}
import com.evolutiongaming.kafka.journal.{Event, JournalError, Payload, PayloadAndType, PayloadType, ToBytes}
import play.api.libs.json.{JsValue, Writes}

import scala.annotation.tailrec

trait EventsToPayload[F[_]] {
  
  def apply(events: Nel[Event]): F[PayloadAndType]
}

object EventsToPayload {

  def apply[F[_] : MonadThrowable](implicit
    eventsToBytes: ToBytes[F, Nel[Event]],
    payloadJsonToBytes: ToBytes[F, PayloadJson]
  ): EventsToPayload[F] = {
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

      payloadAndType(eventJsons(events.toList, List.empty)).handleErrorWith { cause =>
        JournalError(s"EventsToPayload failed for $events: $cause", cause.some).raiseError[F, PayloadAndType]
      }
    }
  }
}
