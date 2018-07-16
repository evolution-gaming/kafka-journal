package com.evolutiongaming.kafka.journal.eventual.cassandra

import com.evolutiongaming.kafka.journal.Alias.{Id, SeqNr}
import com.evolutiongaming.skafka.Topic


// TODO should it have a seqNr and same partitioning as journal
// TODO so we can make the insert and Metadata as atomic operation ? ¯\_(ツ)_/¯
case class Metadata(
  id: Id,
  topic: Topic, // TODO we'd better have this stored, however not needed for reading
  segmentSize: Int, // TODO swap order
  deleteTo: SeqNr) // TODO use DeleteTo.Confirmed // TODO rename to deleteTo

// TODO segmentSize && deleteTo are optional