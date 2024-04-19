package com.evolutiongaming.kafka.journal.conversions

import cats.{Monad, ~>}
import cats.data.{NonEmptyList => Nel}
import cats.syntax.all._
import com.evolutiongaming.catshelper.{MonadThrowable, MeasureDuration}
import com.evolutiongaming.kafka.journal.PayloadAndType._
import com.evolutiongaming.kafka.journal._
import play.api.libs.json.{JsValue, Json, Writes}

import scala.annotation.tailrec

/** Prepare payload for storing into Kafka.
  *
  * Converts a structure convenient for a business logic to a structure, which
  * is convenient to store into Kafka.
  */
trait KafkaWrite[F[_], A] {

  def apply(events: Events[A]): F[PayloadAndType]
}

object KafkaWrite {

  def summon[F[_], A](implicit kafkaWrite: KafkaWrite[F, A]): KafkaWrite[F, A] = kafkaWrite

  implicit def payloadKafkaWrite[F[_] : MonadThrowable](implicit
    eventsToBytes: ToBytes[F, Events[Payload]],
    payloadJsonToBytes: ToBytes[F, PayloadJson[JsValue]]
  ): KafkaWrite[F, Payload] = {
    events: Events[Payload] => {

      /** Safely cast an argument to `Event[Payload.TextOrJson]`,
        * or returns `None` if payload is binary instead.
        */
      def eventJson(event: Event[Payload]): Option[Event[Payload.TextOrJson]] = {
        event.payload.fold {
          event.copy(payload = none[Payload.TextOrJson]).some
        } {
          case _: Payload.Binary     => none[Event[Payload.TextOrJson]]
          case a: Payload.TextOrJson => event.as(a).some
        }
      }

      /** Casts all list elements to `Event[Payload.TextOrJson]` or
        * returns `None` if list contains events with binary payload,
        * 
        * The method may, probably, be simplified as following:
        * ```
        * val onlyTextOrJsonPresent = events.forall { event =>
        *   event.payload.fold(true)(_.isInstanceOf[Payload.TextOrJson])
        * }
        * if (onlyTextOrJsonPresent) events.flatMap(eventJson) else Nil
        * ```
        */
      @tailrec
      def eventJsons(events: List[Event[Payload]], result: List[Event[Payload.TextOrJson]]): List[Event[Payload.TextOrJson]] =
        events match {
          case Nil          => result.reverse
          case head :: tail => eventJson(head) match {
            case Some(x) => eventJsons(tail, x :: result)
            case None    => List.empty[Event[Payload.TextOrJson]]
          }
        }

      def toEventJsonPayload(payload: Payload.TextOrJson) = {

        def of[A : Writes](a: A, payloadType: PayloadType.TextOrJson) =
          EventJsonPayloadAndType(Json.toJson(a), payloadType)

        payload match {
          case a: Payload.Json => of(a.value, PayloadType.Json)
          case a: Payload.Text => of(a.value, PayloadType.Text)
        }
      }

      def payloadAndType(eventJsons: List[Event[Payload.TextOrJson]]) = {
        eventJsons match {
          case head :: tail =>
            val jsonEvents = events.copy(events = Nel(head, tail))
            val jsonKafkaWrite = KafkaWrite.writeJson(toEventJsonPayload, payloadJsonToBytes)
            jsonKafkaWrite(jsonEvents)
          case Nil          =>
            withErrorAdapted(events) {
              eventsToBytes(events).map { PayloadAndType(_, PayloadType.Binary) }
            }
        }
      }

      payloadAndType(eventJsons(events.events.toList, List.empty))
    }
  }

  def writeJson[F[_] : MonadThrowable, A, B](
    toEventJsonPayload: A => EventJsonPayloadAndType[B],
    payloadJsonToBytes: ToBytes[F, PayloadJson[B]]
  ): KafkaWrite[F, A] = {

    events: Events[A] => {

      def eventJson(event: Event[A]): EventJson[B] = {

        def of(payloadType: Option[PayloadType.TextOrJson], payload: Option[B]) = {
          EventJson(event.seqNr, event.tags, payloadType, payload)
        }

        event.payload.fold(of(none, none)) { a =>
          val jsonPayload = toEventJsonPayload(a)
          of(jsonPayload.payloadType.some, jsonPayload.payload.some)
        }
      }

      val eventsJsons = events.events.map(eventJson)
      val payloadJson = PayloadJson(eventsJsons, events.metadata.some)

      withErrorAdapted(events) {
        payloadJsonToBytes(payloadJson)
          .map(PayloadAndType(_, PayloadType.Json))
      }
    }
  }

  private def withErrorAdapted[F[_] : MonadThrowable, A, B](events: Events[A])(fa: F[B]): F[B] =
    fa.adaptError { case e =>
      JournalError(s"KafkaWrite failed for $events: $e", e)
    }

  implicit class KafkaWriteOps[F[_], A](val self: KafkaWrite[F, A]) extends AnyVal {
    def withMetrics(
      metrics: KafkaWriteMetrics[F]
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
