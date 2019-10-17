package com.evolutiongaming.kafka.journal.eventual.cassandra

import com.datastax.driver.core.GettableByNameData
import com.evolutiongaming.kafka.journal.{PartitionOffset, SeqNr}
import com.evolutiongaming.scassandra.DecodeRow
import com.evolutiongaming.scassandra.syntax._


final case class JournalHead(
  partitionOffset: PartitionOffset,
  segmentSize: SegmentSize,
  seqNr: SeqNr,
  deleteTo: Option[SeqNr])

object JournalHead {

  implicit val decodeRowJournalHead: DecodeRow[JournalHead] = {
    row: GettableByNameData => {
      JournalHead(
        partitionOffset = row.decode[PartitionOffset],
        segmentSize = row.decode[SegmentSize],
        seqNr = row.decode[SeqNr],
        deleteTo = row.decode[Option[SeqNr]]("delete_to"))
    }
  }
}