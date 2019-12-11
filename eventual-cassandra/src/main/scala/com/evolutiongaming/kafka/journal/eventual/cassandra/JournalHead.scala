package com.evolutiongaming.kafka.journal.eventual.cassandra

import com.datastax.driver.core.{GettableByNameData, SettableData}
import com.evolutiongaming.kafka.journal.{DeleteTo, PartitionOffset, SeqNr}
import com.evolutiongaming.scassandra.syntax._
import com.evolutiongaming.scassandra.{DecodeRow, EncodeRow}


final case class JournalHead(
  partitionOffset: PartitionOffset,
  segmentSize: SegmentSize,
  seqNr: SeqNr,
  deleteTo: Option[DeleteTo])

object JournalHead {

  implicit val decodeRowJournalHead: DecodeRow[JournalHead] = {
    row: GettableByNameData => {
      JournalHead(
        partitionOffset = row.decode[PartitionOffset],
        segmentSize = row.decode[SegmentSize],
        seqNr = row.decode[SeqNr],
        deleteTo = row.decode[Option[DeleteTo]]("delete_to")) // TODO why require "delete_to" ?
    }
  }

  implicit val encodeRowJournalHead: EncodeRow[JournalHead] = {
    new EncodeRow[JournalHead] {
      def apply[B <: SettableData[B]](data: B, value: JournalHead) = {
        data
          .encode(value.partitionOffset)
          .encode(value.segmentSize)
          .encode(value.seqNr)
          .encodeSome(value.deleteTo)
      }
    }
  }
}