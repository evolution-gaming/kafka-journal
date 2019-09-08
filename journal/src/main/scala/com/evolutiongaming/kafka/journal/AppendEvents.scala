package com.evolutiongaming.kafka.journal

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
    (key: Key, events: Nel[Event], metadata: Option[JsValue], headers: Headers) => {
      for {
        timestamp <- Clock[F].instant
        metadata1  = Metadata(data = metadata)
        action    <- Action.Append.of[F](key, timestamp, origin, events, metadata1, headers) // TODO measure
        result    <- appendAction(action)
      } yield result
    }
  }
}
