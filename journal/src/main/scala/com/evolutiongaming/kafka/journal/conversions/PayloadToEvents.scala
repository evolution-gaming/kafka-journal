package com.evolutiongaming.kafka.journal.conversions

import cats.Monad
import cats.implicits._
import com.evolutiongaming.catshelper.MonadThrowable
import com.evolutiongaming.kafka.journal.PayloadAndType.PayloadJson
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.smetrics.MeasureDuration

trait PayloadToEvents[F[_]] {

  def apply(payloadAndType: PayloadAndType): F[Events]
}

object PayloadToEvents {

  implicit def apply[F[_] : MonadThrowable : FromAttempt : FromJsResult](implicit
    eventsFromBytes: FromBytes[F, Events],
    payloadJsonFromBytes: FromBytes[F, PayloadJson]
  ): PayloadToEvents[F] = {

    payloadAndType: PayloadAndType => {
      val payload = payloadAndType.payload
      val result = payloadAndType.payloadType match {
        case PayloadType.Binary => eventsFromBytes(payload)
        case PayloadType.Json   =>

          def events(payloadJson: PayloadJson) = {
            payloadJson.events.traverse { event =>
              val payloadType = event.payloadType getOrElse PayloadType.Json
              val payload     = event.payload.traverse { payload =>

                def text = {
                  FromJsResult[F]
                    .apply { payload.validate[String] }
                    .map { str => Payload.text(str) }
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
                  seqNr   = event.seqNr,
                  tags    = event.tags,
                  payload = payload)
              }
            }
          }

          for {
            payloadJson <- payloadJsonFromBytes(payload)
            events      <- events(payloadJson)
          } yield {
            Events(
              events,
              payloadJson.metadata getOrElse PayloadMetadata.empty)
          }
      }
      result.adaptError { case e =>
        JournalError(s"PayloadToEvents failed for $payloadAndType: $e", e)
      }
    }
  }

  implicit class PayloadToEventsOps[F[_]](val self: PayloadToEvents[F]) extends AnyVal {
    def withMetrics(
      metrics: PayloadToEventsMetrics[F]
    )(
      implicit F: Monad[F], measureDuration: MeasureDuration[F]
    ): PayloadToEvents[F] = {
      payloadAndType =>
        for {
          d <- MeasureDuration[F].start
          r <- self(payloadAndType)
          d <- d
          _ <- metrics(payloadAndType, d)
        } yield r
    }
  }
}
