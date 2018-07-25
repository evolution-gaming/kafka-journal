package com.evolutiongaming.kafka.journal.eventual.cassandra

import com.evolutiongaming.kafka.journal.Alias.SeqNr


// TODO should it have a seqNr and same partitioning as journal
// TODO so we can make the insert and Metadata as atomic operation ? ¯\_(ツ)_/¯
final case class Metadata(
  segmentSize: Int, // TODO swap order
  deleteTo: SeqNr) // TODO use DeleteTo.Confirmed // TODO rename to deleteTo

// TODO segmentSize && deleteTo are optional