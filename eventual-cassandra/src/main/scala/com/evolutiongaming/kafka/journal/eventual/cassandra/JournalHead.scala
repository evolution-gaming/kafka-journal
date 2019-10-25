package com.evolutiongaming.kafka.journal.eventual.cassandra

import com.datastax.driver.core.{GettableByNameData, SettableData}
import com.evolutiongaming.kafka.journal.{PartitionOffset, SeqNr}
import com.evolutiongaming.scassandra.syntax._
import com.evolutiongaming.scassandra.{DecodeRow, EncodeRow}


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

  implicit val encodeRowJournalHead: EncodeRow[JournalHead] = {
    new EncodeRow[JournalHead] {
      def apply[B <: SettableData[B]](data: B, value: JournalHead) = {
        data
          .encode(value.partitionOffset)
          .encode(value.segmentSize)
          .encode(value.seqNr)
          .encodeSome("delete_to", value.deleteTo)
      }
    }
  }
}