package com.evolutiongaming.kafka.journal

import cats.~>
import com.evolutiongaming.kafka.journal.conversions.KafkaRead
import com.evolutiongaming.kafka.journal.eventual.{EventualPayloadAndType, EventualRead}

trait JournalRead[F[_], A] extends KafkaRead[F, A] with EventualRead[F, A]

object JournalRead {

  def apply[F[_], A](implicit R: JournalRead[F, A]): JournalRead[F, A] = R

  implicit def fromReads[F[_], A](implicit KR: KafkaRead[F, A], ER: EventualRead[F, A]): JournalRead[F, A] =
    new JournalRead[F, A] {
      override def readKafka(payloadAndType: PayloadAndType): F[Events[A]] = KR.readKafka(payloadAndType)
      override def readEventual(payloadAndType: EventualPayloadAndType): F[A] = ER.readEventual(payloadAndType)
    }

  implicit class JournalReadOps[F[_], A](val self: JournalRead[F, A]) extends AnyVal {

    def mapK[G[_]](fg: F ~> G): JournalRead[G, A] = new JournalRead[G, A] {
      override def readKafka(payloadAndType: PayloadAndType): G[Events[A]] =
        (self: KafkaRead[F, A]).mapK(fg).readKafka(payloadAndType)

      override def readEventual(payloadAndType: EventualPayloadAndType): G[A] =
        (self: EventualRead[F, A]).mapK(fg).readEventual(payloadAndType)
    }
  }
}
