package com.evolutiongaming.kafka.journal

final case class Snapshot[A](
  seqNr: SeqNr,
  tags: Tags = Tags.empty,
  payload: Option[A] = None
)
