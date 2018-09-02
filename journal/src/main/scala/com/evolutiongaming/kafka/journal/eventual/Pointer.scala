package com.evolutiongaming.kafka.journal.eventual

import com.evolutiongaming.kafka.journal.{PartitionOffset, SeqNr}

final case class Pointer(seqNr: SeqNr, partitionOffset: PartitionOffset)