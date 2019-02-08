package com.evolutiongaming.kafka.journal.eventual.cassandra

import com.evolutiongaming.kafka.journal.{PartitionOffset, SeqNr}


// TODO add Origin
final case class Head(
  partitionOffset: PartitionOffset,
  segmentSize: Int,
  seqNr: SeqNr,
  deleteTo: Option[SeqNr])