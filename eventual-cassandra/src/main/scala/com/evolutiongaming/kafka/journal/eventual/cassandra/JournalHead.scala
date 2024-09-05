package com.evolutiongaming.kafka.journal.eventual.cassandra

import cats.syntax.all.*
import com.datastax.driver.core.{GettableData, SettableData}
import com.evolutiongaming.kafka.journal.{CorrelationId, DeleteTo, PartitionOffset, SeqNr}
import com.evolutiongaming.scassandra.syntax.*
import com.evolutiongaming.scassandra.{DecodeRow, EncodeRow}
import com.evolutiongaming.scassandra.CodecRow

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

  //format: off
  implicit def journalHeadCodec(implicit //format: on
    encodeExpiry: EncodeRow[Option[Expiry]],
    decodeExpiry: DecodeRow[Option[Expiry]],
  ): CodecRow[JournalHead] = new CodecRow[JournalHead] {

    override def encode[D <: GettableData with SettableData[D]](data: D, value: JournalHead): D = {
      data
        .encode(value.partitionOffset)
        .encode(value.segmentSize)
        .encode(value.seqNr)
        .encodeSome(value.deleteTo)
        .encode(value.expiry)(encodeExpiry)
        .put(value.correlationId)
    }

    override def decode[D <: GettableData](data: D): JournalHead = {
      JournalHead(
        partitionOffset = data.decode[PartitionOffset],
        segmentSize     = data.decode[SegmentSize],
        seqNr           = data.decode[SeqNr],
        deleteTo        = data.decode[Option[DeleteTo]]("delete_to"),
        expiry          = data.decode[Option[Expiry]](decodeExpiry),
        correlationId   = data.take[Option[CorrelationId]],
      )
    }

  }
}
