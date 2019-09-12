package com.evolutiongaming.kafka.journal.conversions

import cats.data.{NonEmptyList => Nel}
import cats.implicits._
import com.evolutiongaming.catshelper.MonadThrowable
import com.evolutiongaming.kafka.journal.PayloadAndType.PayloadJson
import com.evolutiongaming.kafka.journal._

trait PayloadToEvents[F[_]] {

  def apply(payloadAndType: PayloadAndType): F[Nel[Event]]
}

object PayloadToEvents {

  implicit def apply[F[_] : MonadThrowable : FromAttempt : FromJsResult](implicit
    eventsFromBytes: FromBytes[F, Nel[Event]],
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
            events <- events(payloadJson)
          } yield events
      }
      result.handleErrorWith { cause =>
        JournalError(s"PayloadToEvents failed for $payloadAndType: $cause", cause.some).raiseError[F, Nel[Event]]
      }
    }
  }
}
