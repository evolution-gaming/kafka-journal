package com.evolutiongaming.kafka.journal

import cats.Monad
import cats.data.{NonEmptyList => Nel}
import cats.syntax.all._
import com.evolutiongaming.kafka.journal.conversions.KafkaWrite

trait AppendEvents[F[_]] {

  def apply[A](
    key: Key,
    events: Nel[Event[A]],
    metadata: RecordMetadata,
    headers: Headers,
  )(implicit kafkaWrite: KafkaWrite[F, A]): F[PartitionOffset]
}

object AppendEvents {

  def apply[F[_]: Monad](produce: Produce[F]): AppendEvents[F] = new AppendEvents[F] {
    override def apply[A](key: Key, events0: Nel[Event[A]], metadata: RecordMetadata, headers: Headers)(
      implicit kafkaWrite: KafkaWrite[F, A],
    ): F[PartitionOffset] = {

      val events = Events(events0, metadata.payload)
      val range  = SeqRange(from = events0.head.seqNr, to = events0.last.seqNr)
      for {
        payloadAndType <- kafkaWrite(events)
        result         <- produce.append(key, range, payloadAndType, metadata.header, headers)
      } yield result
    }
  }
}
