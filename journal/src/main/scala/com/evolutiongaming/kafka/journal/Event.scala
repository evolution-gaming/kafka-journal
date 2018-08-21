package com.evolutiongaming.kafka.journal

import java.time.Instant

import com.evolutiongaming.kafka.journal.Alias.Tags

// TODO add timestamp ?
final case class Event(
  seqNr: SeqNr,
  tags: Tags = Tags.Empty,
  payload: Bytes = Bytes.Empty)

// TODO rename, statements called records, here - events
// TODO move to eventual ?
final case class ReplicatedEvent(event: Event, timestamp: Instant, partitionOffset: PartitionOffset)