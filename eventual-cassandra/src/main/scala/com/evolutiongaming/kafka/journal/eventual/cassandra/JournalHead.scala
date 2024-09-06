package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.syntax.all.*
import com.datastax.driver.core.{GettableData, GettableByNameData, SettableData}
import com.evolutiongaming.kafka.journal.{CorrelationId, DeleteTo, PartitionOffset, SeqNr}
import com.evolutiongaming.scassandra.syntax.*
import com.evolutiongaming.scassandra.{DecodeRow, EncodeRow, UpdateRow}

/** Represents metadata of particular journal.
  *
  * One piece of information could be found here is the metadata of last action
  * stored to Cassandra.
  *
  * @param partitionOffset the offset where the last stored journal action is located in Kafka
  * @param segmentSize the segmentSize used to write a journal to Cassandra
  * @param seqNr the sequence number of the last event in journal stored in Cassandra
  * @param deleteTo the last sequence number deleted from journal if any
  * @param expiry the expiry time of the journal. On expiry the journal will be purged
  * @param correlationId unique ID of metadata entry, created once and _never_ updated. 
  *   Used to correlate metadata with events inserted while metadata is actual: 
  *   if purging journal will by mistake delete not all events, then after inserting new events
  *   old ones will not be correlated with new metadata thus excluded from recovery. 
  */
final case class JournalHead(
  partitionOffset: PartitionOffset,
  segmentSize: SegmentSize,
  seqNr: SeqNr,
  deleteTo: Option[DeleteTo]           = none,
  expiry: Option[Expiry]               = none,
  correlationId: Option[CorrelationId] = none,
)

object JournalHead {

  implicit def decodeRowJournalHead(implicit decode: DecodeRow[Option[Expiry]]): DecodeRow[JournalHead] = {
    (row: GettableByNameData) =>
      {
        JournalHead(
          partitionOffset = row.decode[PartitionOffset],
          segmentSize     = row.decode[SegmentSize],
          seqNr           = row.decode[SeqNr],
          deleteTo        = row.decode[Option[DeleteTo]]("delete_to"),
          expiry          = row.decode[Option[Expiry]],
          correlationId   = row.decode[Option[CorrelationId]]("properties"),
        )
      }
  }

  implicit def encodeRowJournalHead(implicit encode: EncodeRow[Option[Expiry]]): UpdateRow[JournalHead] = {
    new UpdateRow[JournalHead] {
      def apply[B <: GettableData & SettableData[B]](data: B, value: JournalHead) = {
        data
          .encode(value.partitionOffset)
          .encode(value.segmentSize)
          .encode(value.seqNr)
          .encodeSome(value.deleteTo)
          .encode(value.expiry)
          .update("properties", value.correlationId)
      }
    }
  }
}
