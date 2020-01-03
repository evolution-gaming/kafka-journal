package com.evolutiongaming.kafka.journal

import cats.Monad
import cats.data.{NonEmptyList => Nel}
import cats.implicits._
import com.evolutiongaming.kafka.journal.conversions.EventsToPayload


trait AppendEvents[F[_]] {

  def apply(
    key: Key,
    events: Nel[Event],
    metadata: RecordMetadata,
    headers: Headers
  ): F[PartitionOffset]
}

object AppendEvents {

  def apply[F[_] : Monad](
    produce: Produce[F])(implicit
    eventsToPayload: EventsToPayload[F]
  ): AppendEvents[F] = {
    (key, events0, metadata, headers) => {
      val events = Events(events0, metadata.payload)
      val range = SeqRange(from = events0.head.seqNr, to = events0.last.seqNr)
      for {
        payloadAndType <- eventsToPayload(events)
        result         <- produce.append(key, range, payloadAndType, metadata.header, headers)
      } yield result
    }
  }
}
