package com.evolutiongaming.kafka.journal.eventual

import cats.implicits._
import cats.~>
import com.evolutiongaming.catshelper.MonadThrowable
import com.evolutiongaming.kafka.journal.util.Fail
import com.evolutiongaming.kafka.journal.util.Fail.implicits._
import com.evolutiongaming.kafka.journal.{JournalError, JsonCodec, Payload, PayloadType}

import scala.util.Try

trait EventualRead[F[_], A] {

  def apply(payloadAndType: EventualPayloadAndType): F[A]

}

object EventualRead {

  def summon[F[_], A](implicit eventualRead: EventualRead[F, A]): EventualRead[F, A] = eventualRead

  implicit def payloadEventualRead[F[_]: MonadThrowable](implicit decode: JsonCodec.Decode[F]): EventualRead[F, Payload] = {
    implicit val fail: Fail[F] = Fail.lift[F]

    payloadAndType => {
      payloadAndType.payloadType match {
        case PayloadType.Binary => liftError(payloadAndType) {
          payloadAndType.payloadBytes[F].map(Payload.binary)
        }

        case PayloadType.Text => liftError(payloadAndType) {
          payloadAndType.payloadStr[F].map(Payload.text)
        }

        case PayloadType.Json =>
          val jsonEventualRead = EventualRead.readJson(decode.fromStr(_).map(Payload(_)))
          jsonEventualRead(payloadAndType)
      }
    }
  }

  def readJson[F[_]: MonadThrowable, A](parsePayload: String => F[A]): EventualRead[F, A] = {
    implicit val fail: Fail[F] = Fail.lift[F]

    payloadAndType => {
      payloadAndType.payloadType match {
        case PayloadType.Json =>
          liftError(payloadAndType) {
            payloadAndType.payloadStr[F].flatMap(parsePayload)
          }

        case other => s"Json payload type expected, got: $other".fail
      }
    }
  }

  private def liftError[F[_]: MonadThrowable, A](payloadAndType: EventualPayloadAndType)(fa: F[A]): F[A] =
    fa.adaptError { case e =>
      JournalError(s"EventualRead failed for $payloadAndType: $e", e)
    }

  implicit class EventualReadOps[F[_], A](val self: EventualRead[F, A]) extends AnyVal {

    def mapK[G[_]](fg: F ~> G): EventualRead[G, A] =
      payloadAndType => fg(self(payloadAndType))
  }

}
