package com.evolutiongaming.kafka.journal.ally.cassandra

import com.evolutiongaming.kafka.journal.Alias.SeqNr

case class SeqNrRange(from: SeqNr, to: SeqNr)
