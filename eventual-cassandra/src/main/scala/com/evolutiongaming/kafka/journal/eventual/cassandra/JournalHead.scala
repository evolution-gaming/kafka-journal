package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.syntax.all._
import com.datastax.driver.core.{GettableByNameData, SettableData}
import com.evolutiongaming.kafka.journal.{DeleteTo, PartitionOffset, SeqNr}
import com.evolutiongaming.scassandra.syntax._
import com.evolutiongaming.scassandra.{DecodeRow, EncodeRow}

/** Represents metadata of particular journal.
  *
  * One piece of information could be found here is the metadata of last action stored to Cassandra.
  *
  * @param partitionOffset
  *   the offset where the last stored journal action is located in Kafka
  * @param segmentSize
  *   the segmentSize used to write a journal to Cassandra
  * @param seqNr
  *   the sequence number of the last event in journal stored in Cassandra
  * @param deleteTo
  *   the last sequence number deleted from journal if any
  */
final case class JournalHead(
  partitionOffset: PartitionOffset,
  segmentSize: SegmentSize,
  seqNr: SeqNr,
  deleteTo: Option[DeleteTo] = none,
  expiry: Option[Expiry] = none,
)

object JournalHead {

  implicit def decodeRowJournalHead(implicit decode: DecodeRow[Option[Expiry]]): DecodeRow[JournalHead] = {
    (row: GettableByNameData) =>
      JournalHead(
        partitionOffset = row.decode[PartitionOffset],
        segmentSize = row.decode[SegmentSize],
        seqNr = row.decode[SeqNr],
        deleteTo = row.decode[Option[DeleteTo]]("delete_to"),
        expiry = row.decode[Option[Expiry]],
      )
  }

  implicit def encodeRowJournalHead(implicit encode: EncodeRow[Option[Expiry]]): EncodeRow[JournalHead] =
    new EncodeRow[JournalHead] {
      def apply[B <: SettableData[B]](data: B, value: JournalHead) =
        data
          .encode(value.partitionOffset)
          .encode(value.segmentSize)
          .encode(value.seqNr)
          .encodeSome(value.deleteTo)
          .encode(value.expiry)
    }
}
