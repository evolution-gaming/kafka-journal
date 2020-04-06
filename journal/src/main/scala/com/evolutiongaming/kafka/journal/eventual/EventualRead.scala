package com.evolutiongaming.kafka.journal.eventual

import cats.~>
import cats.implicits._
import com.evolutiongaming.catshelper.MonadThrowable
import com.evolutiongaming.kafka.journal.{JournalError, JsonCodec, Payload, PayloadType}

import scala.util.Try

trait EventualRead[F[_], A] {

  def readEventual(payloadAndType: EventualPayloadAndType): F[A]

}

object EventualRead {

  def apply[F[_], A](implicit R: EventualRead[F, A]): EventualRead[F, A] = R

  implicit def forPayload[F[_] : MonadThrowable](implicit decode: JsonCodec.Decode[Try]): EventualRead[F, Payload] =
    payloadAndType => {
      val payload = payloadAndType.payload

      def bytesFromPayload = payload.toOption.liftTo[F](new JournalError("Bytes expected, but got string"))
      def stringFromPayload = payload.swap.toOption.liftTo[F](new JournalError("String expected, but got bytes"))

      val result = payloadAndType.payloadType match {
        case PayloadType.Binary => bytesFromPayload.map(Payload.binary)
        case PayloadType.Text   => stringFromPayload.map(Payload.text)
        case PayloadType.Json   => stringFromPayload.flatMap(decode.fromStr(_).liftTo[F]).map(Payload(_))
      }

      result.adaptError { case e =>
        JournalError(s"EventualRead failed for $payloadAndType: $e", e)
      }
    }


  implicit class EventualReadOps[F[_], A](val self: EventualRead[F, A]) extends AnyVal {

    def mapK[G[_]](fg: F ~> G): EventualRead[G, A] =
      payloadAndType => fg(self.readEventual(payloadAndType))
  }

}
