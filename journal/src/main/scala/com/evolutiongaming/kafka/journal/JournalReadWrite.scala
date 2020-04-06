package com.evolutiongaming.kafka.journal

import com.evolutiongaming.kafka.journal.conversions.KafkaWrite
import com.evolutiongaming.kafka.journal.eventual.EventualPayloadAndType

trait JournalReadWrite[F[_], A] extends JournalRead[F, A] with KafkaWrite[F, A]

object JournalReadWrite {

  def apply[F[_], A](implicit RW: JournalReadWrite[F, A]): JournalReadWrite[F, A] = RW

  implicit def fromReadWrite[F[_], A](implicit R: JournalRead[F, A], W: KafkaWrite[F, A]): JournalReadWrite[F, A] =
    new JournalReadWrite[F, A] {
      override def writeKafka(events: Events[A]): F[PayloadAndType] =
        W.writeKafka(events)

      override def readKafka(payloadAndType: PayloadAndType): F[Events[A]] =
        R.readKafka(payloadAndType)

      override def readEventual(payloadAndType: EventualPayloadAndType): F[A] =
        R.readEventual(payloadAndType)
    }
}
