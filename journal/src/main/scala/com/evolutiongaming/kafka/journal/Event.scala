package com.evolutiongaming.kafka.journal

import com.evolutiongaming.kafka.journal.Alias.{Bytes, SeqNr}

case class Event(payload: Bytes, seqNr: SeqNr, tags: Set[String])
