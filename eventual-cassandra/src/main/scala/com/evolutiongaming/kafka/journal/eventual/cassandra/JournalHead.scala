package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.syntax.all.*
import com.datastax.driver.core.{GettableByNameData, SettableData}
import com.evolutiongaming.kafka.journal.{DeleteTo, PartitionOffset, SeqNr}
import com.evolutiongaming.scassandra.syntax.*
import com.evolutiongaming.scassandra.{DecodeRow, EncodeRow}

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
  * @param recordId unique ID of metadata entry, created once and _never_ updated. 
  *   Used to correlate metadata with events inserted while metadata is actual: 
  *   if purging journal will by mistake delete not all events, then after inserting new events
  *   old ones will not be correlated with new metadata thus excluded from recovery.
  *   The field is optional, because it was added later and old records do not have it,
  *   thus while al least one record in `metajournal` table does not have `record_id` field - it should be optional.
  *   `metajournal` record can be _created_ with `record_id` field, but never updated! 
  */
private[journal] final case class JournalHead(
  partitionOffset: PartitionOffset,
  segmentSize: SegmentSize,
  seqNr: SeqNr,
  deleteTo: Option[DeleteTo] = none,
  expiry: Option[Expiry]     = none,
  recordId: Option[RecordId] = none,
)

private[journal] object JournalHead {

  implicit def decodeRowJournalHead(implicit decode: DecodeRow[Option[Expiry]]): DecodeRow[JournalHead] = {
    (row: GettableByNameData) =>
      {
        JournalHead(
          partitionOffset = row.decode[PartitionOffset],
          segmentSize     = row.decode[SegmentSize],
          seqNr           = row.decode[SeqNr],
          deleteTo        = row.decode[Option[DeleteTo]]("delete_to"),
          expiry          = row.decode[Option[Expiry]],
          recordId        = row.decode[Option[RecordId]]("record_id"),
        )
      }
  }

  implicit def encodeRowJournalHead(implicit encode: EncodeRow[Option[Expiry]]): EncodeRow[JournalHead] = {
    new EncodeRow[JournalHead] {
      def apply[B <: SettableData[B]](data: B, value: JournalHead) = {
        data
          .encode(value.partitionOffset)
          .encode(value.segmentSize)
          .encode(value.seqNr)
          .encodeSome(value.deleteTo)
          .encode(value.expiry)
          .encodeSome("record_id", value.recordId)
      }
    }
  }
}
