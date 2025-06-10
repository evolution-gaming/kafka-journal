package com.evolutiongaming.kafka.journal.eventual

import cats.syntax.all.*
import com.evolutiongaming.catshelper.MonadThrowable
import com.evolutiongaming.kafka.journal.{JournalError, JsonCodec, Payload}
import scodec.bits.ByteVector

/**
 * Prepare payload for storing.
 *
 * Converts a structure convenient for a business logic to a structure, which is convenient to store
 * to eventual store, i.e. Cassandra.
 */
trait EventualWrite[F[_], A] {

  def apply(payload: A): F[EventualPayloadAndType]

}

object EventualWrite {

  def summon[F[_], A](
    implicit
    eventualWrite: EventualWrite[F, A],
  ): EventualWrite[F, A] = eventualWrite

  implicit def forPayload[F[_]: MonadThrowable](
    implicit
    encode: JsonCodec.Encode[F],
  ): EventualWrite[F, Payload] =
    payload => {
      val eventualPayload = payload match {
        case payload: Payload.Binary => payload.value.asRight[String].pure[F]
        case payload: Payload.Text => payload.value.asLeft[ByteVector].pure[F]
        case payload: Payload.Json => encode.toStr(payload.value).map(_.asLeft[ByteVector])
      }

      eventualPayload
        .map(EventualPayloadAndType(_, payload.payloadType))
        .adaptError {
          case e =>
            JournalError(s"EventualWrite failed for $payload: $e", e)
        }
    }

}
