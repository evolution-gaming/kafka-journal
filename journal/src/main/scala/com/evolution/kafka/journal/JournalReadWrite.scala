package com.evolution.kafka.journal

import com.evolution.kafka.journal.conversions.{KafkaRead, KafkaWrite}
import com.evolution.kafka.journal.eventual.EventualRead

final case class JournalReadWrite[F[_], A](
  kafkaRead: KafkaRead[F, A],
  kafkaWrite: KafkaWrite[F, A],
  eventualRead: EventualRead[F, A],
)

object JournalReadWrite {

  def of[F[_], A](
    implicit
    kafkaRead: KafkaRead[F, A],
    kafkaWrite: KafkaWrite[F, A],
    eventualRead: EventualRead[F, A],
  ): JournalReadWrite[F, A] = {
    JournalReadWrite(kafkaRead, kafkaWrite, eventualRead)
  }
}
