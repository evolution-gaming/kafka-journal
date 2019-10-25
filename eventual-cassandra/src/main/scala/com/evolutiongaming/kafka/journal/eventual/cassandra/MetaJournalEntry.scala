package com.evolutiongaming.kafka.journal.eventual.cassandra

import java.time.Instant

import com.evolutiongaming.kafka.journal.Origin

final case class MetaJournalEntry(
  journalHead: JournalHead,
  created: Instant,
  updated: Instant,
  origin: Option[Origin])