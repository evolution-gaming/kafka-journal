package com.evolutiongaming.kafka.journal.eventual.cassandra

import com.evolutiongaming.kafka.journal.{Origin, PartitionOffset, SeqNr}


// TODO looks like we also need to store offset
// TODO store partition/offset #57
// TODO rename to topic metadata
// TODO add Origin
final case class Metadata(
  partitionOffset: PartitionOffset,
  segmentSize: Int, // TODO swap order
  seqNr: SeqNr,
  deleteTo: Option[SeqNr])

//final case class UpdateMetadata()