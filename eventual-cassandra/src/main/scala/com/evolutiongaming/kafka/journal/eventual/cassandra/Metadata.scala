package com.evolutiongaming.kafka.journal.eventual.cassandra

import com.evolutiongaming.kafka.journal.SeqNr


// TODO should it have a seqNr and same partitioning as journal
// TODO so we can make the insert and Metadata as atomic operation ? ¯\_(ツ)_/¯
// TODO use DeleteTo.Confirmed // TODO rename to deleteTo
// TODO looks like we also need to store offset
final case class Metadata(
  segmentSize: Int, // TODO swap order
  deleteTo: Option[SeqNr])

// TODO segmentSize && deleteTo are optional