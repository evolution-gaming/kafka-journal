package com.evolutiongaming.kafka.journal.conversions

import cats.syntax.all._
import cats.{Monad, ~>}
import com.evolutiongaming.catshelper._
import com.evolutiongaming.kafka.journal.PayloadAndType._
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.kafka.journal.util.Fail
import com.evolutiongaming.smetrics
import play.api.libs.json.JsValue

trait KafkaRead[F[_], A] {

  def apply(payloadAndType: PayloadAndType): F[Events[A]]
}

object KafkaRead {

  def summon[F[_], A](implicit kafkaRead: KafkaRead[F, A]): KafkaRead[F, A] = kafkaRead

  implicit def payloadKafkaRead[F[_]: MonadThrowable: FromJsResult](implicit
    eventsFromBytes: FromBytes[F, Events[Payload]],
    payloadJsonFromBytes: FromBytes[F, PayloadJson[JsValue]]
  ): KafkaRead[F, Payload] = {

    payloadAndType: PayloadAndType => {

      def fromEventJsonPayload(jsonPayload: EventJsonPayloadAndType[JsValue]) = {
        def text = {
          FromJsResult[F]
            .apply { jsonPayload.payload.validate[String] }
            .map { str => Payload.text(str) }
        }

        jsonPayload.payloadType match {
          case PayloadType.Json => Payload.json(jsonPayload.payload).pure[F]
          case PayloadType.Text => text
        }
      }

      payloadAndType.payloadType match {
        case PayloadType.Binary =>
          withErrorAdapted(payloadAndType) {
            eventsFromBytes(payloadAndType.payload)
          }

        case PayloadType.Json =>
          val jsonKafkaRead = KafkaRead.readJson(payloadJsonFromBytes, fromEventJsonPayload)
          jsonKafkaRead(payloadAndType)
      }
    }
  }

  def readJson[F[_]: MonadThrowable, A, B](
    payloadJsonFromBytes: FromBytes[F, PayloadJson[A]],
    fromEventJsonPayload: EventJsonPayloadAndType[A] => F[B]
  ): KafkaRead[F, B] = {

    payloadAndType: PayloadAndType => {

      def events(payloadJson: PayloadJson[A]) = {
        payloadJson.events.traverse { event =>
          val payloadType = event.payloadType getOrElse PayloadType.Json
          val payload = event.payload
            .map(EventJsonPayloadAndType(_, payloadType))
            .traverse(fromEventJsonPayload)

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

      val payload = payloadAndType.payload
      val result = payloadAndType.payloadType match {
        case PayloadType.Json =>
          for {
            payloadJson <- payloadJsonFromBytes(payload)
            events      <- events(payloadJson)
          } yield {
            Events(
              events,
              payloadJson.metadata getOrElse PayloadMetadata.empty
            )
          }

        case other =>
          Fail.lift[F].fail[Events[B]](s"Only Json payload type supported, got: $other")
      }

      withErrorAdapted(payloadAndType)(result)
    }
  }

  private def withErrorAdapted[F[_]: ApplicativeThrowable, A](payloadAndType: PayloadAndType)(fa: F[A]): F[A] =
    fa.adaptErr { case e =>
      JournalError(s"KafkaRead failed for $payloadAndType: $e", e)
    }

  implicit class KafkaReadOps[F[_], A](val self: KafkaRead[F, A]) extends AnyVal {

    @deprecated("Use `withMetrics1` instead", "2.2.0")
    def withMetrics(
      metrics: KafkaReadMetrics[F]
    )(
      implicit F: Monad[F], measureDuration: smetrics.MeasureDuration[F]
    ): KafkaRead[F, A] = {
      withMetrics1(metrics)(F, measureDuration.toCatsHelper)
    }

    def withMetrics1(
      metrics: KafkaReadMetrics[F]
    )(
      implicit F: Monad[F], measureDuration: MeasureDuration[F]
    ): KafkaRead[F, A] = {
      payloadAndType =>
        for {
          d <- MeasureDuration[F].start
          r <- self(payloadAndType)
          d <- d
          _ <- metrics(payloadAndType, d)
        } yield r
    }

    def mapK[G[_]](fg: F ~> G): KafkaRead[G, A] =
      payloadAndType => fg(self(payloadAndType))
  }
}
