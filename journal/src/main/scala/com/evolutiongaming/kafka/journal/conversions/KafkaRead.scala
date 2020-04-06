package com.evolutiongaming.kafka.journal.conversions

import cats.{Monad, ~>}
import cats.implicits._
import com.evolutiongaming.catshelper.MonadThrowable
import com.evolutiongaming.kafka.journal.PayloadAndType.PayloadJson
import com.evolutiongaming.kafka.journal._
import com.evolutiongaming.smetrics.MeasureDuration

trait KafkaRead[F[_], A] {

  def readKafka(payloadAndType: PayloadAndType): F[Events[A]]
}

object KafkaRead {

  def apply[F[_], A](implicit R: KafkaRead[F, A]): KafkaRead[F, A] = R

  implicit def forPayload[F[_] : MonadThrowable : FromAttempt : FromJsResult](implicit
    eventsFromBytes: FromBytes[F, Events[Payload]],
    payloadJsonFromBytes: FromBytes[F, PayloadJson]
  ): KafkaRead[F, Payload] = {

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
        JournalError(s"KafkaRead failed for $payloadAndType: $e", e)
      }
    }
  }

  implicit class KafkaReadOps[F[_], A](val self: KafkaRead[F, A]) extends AnyVal {
    def withMetrics(
      metrics: PayloadToEventsMetrics[F]
    )(
      implicit F: Monad[F], measureDuration: MeasureDuration[F]
    ): KafkaRead[F, A] = {
      payloadAndType =>
        for {
          d <- MeasureDuration[F].start
          r <- self.readKafka(payloadAndType)
          d <- d
          _ <- metrics(payloadAndType, d)
        } yield r
    }

    def mapK[G[_]](fg: F ~> G): KafkaRead[G, A] =
      payloadAndType => fg(self.readKafka(payloadAndType))
  }
}
