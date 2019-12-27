package com.evolutiongaming.kafka.journal

import cats.Monad
import cats.data.{NonEmptyList => Nel}
import cats.implicits._
import com.evolutiongaming.kafka.journal.conversions.EventsToPayload
import play.api.libs.json.JsValue


trait AppendEvents[F[_]] {

  def apply(
    key: Key,
    events: Nel[Event],
    expireAfter: Option[ExpireAfter],
    metadata: Option[JsValue],
    headers: Headers
  ): F[PartitionOffset]
}

object AppendEvents {

  def apply[F[_] : Monad](
    produce: Produce[F])(implicit
    eventsToPayload: EventsToPayload[F]
  ): AppendEvents[F] = {
    (key, events0, expireAfter, metadata, headers) => {
      val events = Events(events0, PayloadMetadata.empty /*TODO expiry: pass metadata*/)
      val range = SeqRange(from = events0.head.seqNr, to = events0.last.seqNr)
      for {
        payloadAndType <- eventsToPayload(events)
        recordMetadata  = RecordMetadata(HeaderMetadata(metadata), PayloadMetadata.empty)
        result         <- produce.append(key, range, payloadAndType, expireAfter, recordMetadata, headers)
      } yield result
    }
  }
}
