package com.evolutiongaming.kafka.journal.eventual.cassandra

import com.evolutiongaming.kafka.journal.SeqNr


// TODO looks like we also need to store offset
// TODO store partition/offset #57
final case class Metadata(
  segmentSize: Int, // TODO swap order
  seqNr: SeqNr,
  deleteTo: Option[SeqNr])