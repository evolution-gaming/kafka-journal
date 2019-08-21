package com.evolutiongaming.kafka.journal

final case class Event(
  seqNr: SeqNr,
  tags: Tags = Tags.Empty,
  payload: Option[Payload] = None)