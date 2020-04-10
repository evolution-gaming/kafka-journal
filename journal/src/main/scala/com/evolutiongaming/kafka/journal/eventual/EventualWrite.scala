package com.evolutiongaming.kafka.journal.eventual

import cats.implicits._
import com.evolutiongaming.catshelper.MonadThrowable
import com.evolutiongaming.kafka.journal.{JournalError, JsonCodec, Payload}
import scodec.bits.ByteVector


trait EventualWrite[F[_], A] {

  def writeEventual(payload: A): F[EventualPayloadAndType]

}

object EventualWrite {

  def apply[F[_], A](implicit W: EventualWrite[F, A]): EventualWrite[F, A] = W

  implicit def forPayload[F[_] : MonadThrowable](implicit encode: JsonCodec.Encode[F]): EventualWrite[F, Payload] =
    payload => {
      val eventualPayload = payload match {
        case payload: Payload.Binary => payload.value.asRight[String].pure[F]
        case payload: Payload.Text   => payload.value.asLeft[ByteVector].pure[F]
        case payload: Payload.Json   => encode.toStr(payload.value).map(_.asLeft[ByteVector])
      }

      eventualPayload
        .map(EventualPayloadAndType(_, payload.payloadType))
        .adaptError { case e =>
          JournalError(s"EventualWrite failed for $payload: $e", e)
        }
    }

}
