package com.evolutiongaming.kafka.journal.eventual.cassandra

import java.time.Instant

import com.datastax.driver.core.{GettableByNameData, SettableData}
import com.evolutiongaming.kafka.journal.Origin
import com.evolutiongaming.scassandra.syntax._
import com.evolutiongaming.scassandra.{DecodeRow, EncodeRow}

final case class MetaJournalEntry(
  journalHead: JournalHead,
  created: Instant,
  updated: Instant,
  origin: Option[Origin])

object MetaJournalEntry {

  implicit val decodeRowMetaJournalEntry: DecodeRow[MetaJournalEntry] = {
    row: GettableByNameData => {
      MetaJournalEntry(
        journalHead = row.decode[JournalHead],
        created = row.decode[Instant]("created"),
        updated = row.decode[Instant]("updated"),
        origin = row.decode[Option[Origin]])
    }
  }

  implicit val encodeRowMetaJournalEntry: EncodeRow[MetaJournalEntry] = {
    new EncodeRow[MetaJournalEntry] {
      def apply[B <: SettableData[B]](data: B, value: MetaJournalEntry) = {
        data
          .encode(value.journalHead)
          .encode("created", value.created)
          .encode("updated", value.updated)
          .encodeSome(value.origin)
      }
    }
  }
}