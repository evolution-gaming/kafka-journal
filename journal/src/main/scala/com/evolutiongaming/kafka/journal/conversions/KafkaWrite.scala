package com.evolutiongaming.kafka.journal.conversions

import cats.{Monad, ~>}
import cats.data.{NonEmptyList => Nel}
import cats.implicits._
import com.evolutiongaming.catshelper.MonadThrowable
import com.evolutiongaming.kafka.journal.PayloadAndType.{EventJson, PayloadJson}
import com.evolutiongaming.kafka.journal.{Event, Events, JournalError, Payload, PayloadAndType, PayloadType, ToBytes}
import com.evolutiongaming.smetrics.MeasureDuration
import play.api.libs.json.{JsValue, Writes}

import scala.annotation.tailrec

trait KafkaWrite[F[_], A] {

  def apply(events: Events[A]): F[PayloadAndType]
}

object KafkaWrite {

  def apply[F[_], A](implicit kafkaWrite: KafkaWrite[F, A]): KafkaWrite[F, A] = kafkaWrite

  implicit def forPayload[F[_] : MonadThrowable](implicit
    eventsToBytes: ToBytes[F, Events[Payload]],
    payloadJsonToBytes: ToBytes[F, PayloadJson]
  ): KafkaWrite[F, Payload] = {
    events: Events[Payload] => {

      def eventJson(head: Event[Payload]) = {

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
      def eventJsons(events: List[Event[Payload]], result: List[EventJson]): List[EventJson] = {
        events match {
          case Nil          => result.reverse
          case head :: tail => eventJson(head) match {
            case Some(x) => eventJsons(tail, x :: result)
            case None    => List.empty[EventJson]
          }
        }
      }

      def metadata = events.metadata

      def payloadAndType(eventJsons: List[EventJson]) = {
        eventJsons match {
          case head :: tail =>
            val events = Nel(head, tail)
            val payload = PayloadJson(events, metadata.some)
            payloadJsonToBytes(payload).map { PayloadAndType(_, PayloadType.Json) }
          case Nil          =>
            eventsToBytes(events).map { PayloadAndType(_, PayloadType.Binary) }
        }
      }

      payloadAndType(eventJsons(events.events.toList, List.empty)).adaptError { case e =>
        JournalError(s"KafkaWrite failed for $events: $e", e)
      }
    }
  }

  implicit class KafkaWriteOps[F[_], A](val self: KafkaWrite[F, A]) extends AnyVal {
    def withMetrics(
      metrics: EventsToPayloadMetrics[F]
    )(
      implicit F: Monad[F], measureDuration: MeasureDuration[F]
    ): KafkaWrite[F, A] = {
      events =>
        for {
          d <- MeasureDuration[F].start
          r <- self(events)
          d <- d
          _ <- metrics(events, r, d)
        } yield r
    }

    def mapK[G[_]](fg: F ~> G): KafkaWrite[G, A] =
      (events: Events[A]) => fg(self(events))
  }
}
