package com.evolutiongaming.kafka.journal

import java.time.Instant

import cats.FlatMap
import cats.data.{NonEmptyList => Nel}
import cats.effect.Clock
import cats.implicits._
import com.evolutiongaming.catshelper.ClockHelper._
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

  def apply[F[_] : FlatMap : Clock](
    appendAction: AppendAction[F],
    origin: Option[Origin])(implicit
    eventsToPayload: EventsToPayload[F]
  ): AppendEvents[F] = {
    (key, events, expireAfter, metadata, headers) => {

      def action(timestamp: Instant) = Action.Append.of[F](
        key = key,
        timestamp = timestamp,
        origin = origin,
        events = Events(events),
        expireAfter = expireAfter,
        metadata = RecordMetadata(data = metadata),
        headers = headers)

      for {
        timestamp <- Clock[F].instant
        action    <- action(timestamp) // TODO measure
        result    <- appendAction(action)
      } yield result
    }
  }
}
