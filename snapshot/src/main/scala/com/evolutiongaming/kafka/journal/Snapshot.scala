package com.evolutiongaming.kafka.journal

final case class Snapshot[A](
  seqNr: SeqNr,
  payload: Option[A] = None
)
