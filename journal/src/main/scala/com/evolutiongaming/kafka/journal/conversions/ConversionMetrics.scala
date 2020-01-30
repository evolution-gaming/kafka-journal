package com.evolutiongaming.kafka.journal.conversions

final case class ConversionMetrics[F[_]](
  payloadToEvents: PayloadToEventsMetrics[F],
  eventsToPayload: EventsToPayloadMetrics[F],
)
