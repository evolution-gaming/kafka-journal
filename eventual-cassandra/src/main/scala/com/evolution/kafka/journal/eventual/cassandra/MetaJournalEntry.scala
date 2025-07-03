package com.evolution.kafka.journal.eventual.cassandra

import com.datastax.driver.core.{GettableByNameData, SettableData}
import com.evolution.kafka.journal.Origin
import com.evolution.kafka.journal.cassandra.OriginExtension.*
import com.evolutiongaming.scassandra.syntax.*
import com.evolutiongaming.scassandra.{DecodeRow, EncodeRow}

import java.time.Instant

private[journal] final case class MetaJournalEntry(
  journalHead: JournalHead,
  created: Instant,
  updated: Instant,
  origin: Option[Origin],
)

private[journal] object MetaJournalEntry {

  implicit def decodeRowMetaJournalEntry(
    implicit
    decode: DecodeRow[JournalHead],
  ): DecodeRow[MetaJournalEntry] = {
    (row: GettableByNameData) =>
      {
        MetaJournalEntry(
          journalHead = row.decode[JournalHead],
          created = row.decode[Instant]("created"),
          updated = row.decode[Instant]("updated"),
          origin = row.decode[Option[Origin]],
        )
      }
  }

  implicit def encodeRowMetaJournalEntry(
    implicit
    encode: EncodeRow[JournalHead],
  ): EncodeRow[MetaJournalEntry] = {
    new EncodeRow[MetaJournalEntry] {
      def apply[B <: SettableData[B]](data: B, value: MetaJournalEntry): B = {
        data
          .encode(value.journalHead)
          .encode("created", value.created)
          .encode("updated", value.updated)
          .encodeSome(value.origin)
      }
    }
  }
}
