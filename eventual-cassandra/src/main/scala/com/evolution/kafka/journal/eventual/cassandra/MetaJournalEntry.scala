package com.evolution.kafka.journal.eventual.cassandra

import com.datastax.oss.driver.api.core.data.{GettableByName, SettableByName}
import com.evolution.kafka.journal.Origin
import com.evolution.kafka.journal.cassandra.OriginExtension.*
import com.evolution.scassandra.syntax.*
import com.evolution.scassandra.{DecodeRow, EncodeRow}

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
    (row: GettableByName) =>
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
      def apply[B <: SettableByName[B]](data: B, value: MetaJournalEntry): B = {
        data
          .encode(value.journalHead)
          .encode("created", value.created)
          .encode("updated", value.updated)
          .encodeSome(value.origin)
      }
    }
  }
}
